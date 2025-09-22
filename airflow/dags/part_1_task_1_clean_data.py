"""Airflow DAG that prepares cleaned datasets and loads them into PostgreSQL."""

from datetime import datetime
from pathlib import Path
from typing import NoReturn

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd

from utils.common import (
    data_root,
    read_csv_file,
    read_parquet_file,
    write_parquet_file,
)

PLAYERS_FILE = 'players.parquet'
AFFILIATES_FILE = 'affiliates.parquet'
TRANSACTIONS_FILE = 'transactions.parquet'
RAW_PLAYERS_CSV = 'players.csv'
RAW_AFFILIATES_CSV = 'affiliates.csv'
RAW_TRANSACTIONS_CSV = 'transactions.csv'
POSTGRES_CONN_ID = 'postgres_default'
SCHEMA_NAME = 'data'


def _expanded_path(filename: str) -> Path:
    return data_root() / 'expanded' / filename


def _transformed_path(filename: str) -> Path:
    return data_root() / 'transformed' / filename


def _deduplicate(frame: pd.DataFrame, subset: str, sort_by: str) -> pd.DataFrame:
    frame = frame.sort_values(by=sort_by)
    return frame.drop_duplicates(subset=subset, keep='last')


@task
def prepare_players_dataset() -> str:
    """Prepare cleaned players dataset from expanded CSV files.

    Returns
    -------
    str
        Path to cleaned players parquet file.
    """
    players = read_csv_file(_expanded_path(RAW_PLAYERS_CSV))
    affiliates = read_csv_file(_expanded_path(RAW_AFFILIATES_CSV))

    players = _deduplicate(players, subset='id', sort_by='updated_at')
    affiliates = _deduplicate(affiliates, subset='id', sort_by='redeemed_at')

    valid_affiliate_ids = set(affiliates['id'].dropna())
    players.loc[
        ~players['affiliate_id'].isin(valid_affiliate_ids),
        'affiliate_id',
    ] = pd.NA

    players = players.sort_values(['created_at', 'id'])
    duplicate_affiliates = players['affiliate_id'].notna() & players.duplicated(
        subset='affiliate_id', keep='first',
    )
    players.loc[duplicate_affiliates, 'affiliate_id'] = pd.NA

    cleaned_players = players[[
        'id',
        'affiliate_id',
        'country_code',
        'is_kyc_approved',
        'created_at',
        'updated_at',
    ]]
    cleaned_players['id'] = pd.to_numeric(
        cleaned_players['id'],
        errors='coerce',
    ).astype('Int64')
    cleaned_players['affiliate_id'] = pd.to_numeric(
        cleaned_players['affiliate_id'], errors='coerce',
    ).astype('Int64')
    cleaned_players['created_at'] = pd.to_datetime(
        cleaned_players['created_at'],
        utc=True,
        errors='coerce',
    )
    cleaned_players['updated_at'] = pd.to_datetime(
        cleaned_players['updated_at'],
        utc=True,
        errors='coerce',
    )

    return write_parquet_file(cleaned_players, _transformed_path(PLAYERS_FILE))


@task
def prepare_affiliates_dataset(players_path: str) -> str:
    """Prepare cleaned affiliates dataset linking to player data.

    Parameters
    ----------
    players_path : str
        Path to cleaned players parquet file.

    Returns
    -------
    str
        Path to cleaned affiliates parquet file.
    """
    affiliates = read_csv_file(_expanded_path(RAW_AFFILIATES_CSV))
    affiliates = _deduplicate(affiliates, subset='id', sort_by='redeemed_at')

    players = read_parquet_file(players_path)
    linked_affiliates = players[['affiliate_id', 'created_at']].dropna().rename(
        columns={'created_at': 'player_created_at'},
    )

    merged = affiliates.merge(
        linked_affiliates,
        left_on='id',
        right_on='affiliate_id',
        how='left',
    )

    merged['id'] = pd.to_numeric(merged['id'], errors='coerce')
    merged['id'] = merged['id'].astype('Int64')
    raw_redeemed = pd.to_datetime(merged['redeemed_at'], utc=True, errors='coerce')
    player_redeemed = pd.to_datetime(
        merged['player_created_at'],
        utc=True,
        errors='coerce',
    )
    merged['redeemed_at'] = player_redeemed.combine_first(raw_redeemed)

    cleaned_affiliates = merged[['id', 'code', 'origin', 'redeemed_at']]

    return write_parquet_file(cleaned_affiliates, _transformed_path(AFFILIATES_FILE))


@task
def prepare_transactions_dataset(players_path: str) -> str:
    """Prepare cleaned transactions dataset with KYC validation.

    Parameters
    ----------
    players_path : str
        Path to cleaned players parquet file.

    Returns
    -------
    str
        Path to cleaned transactions parquet file.
    """
    transactions = read_csv_file(_expanded_path(RAW_TRANSACTIONS_CSV))
    transactions = _deduplicate(transactions, subset='id', sort_by='timestamp')

    players = read_parquet_file(players_path)

    merged = transactions.merge(
        players[[
            'id',
            'is_kyc_approved',
            'created_at',
            'updated_at',
        ]].rename(columns={'id': 'player_id'}),
        on='player_id',
        how='inner',
    )

    kyc_mask = merged['is_kyc_approved'].astype(str).str.upper().eq('TRUE')
    cleaned = merged[kyc_mask]
    reference_time = cleaned['created_at']
    cleaned = cleaned[cleaned['timestamp'] >= reference_time]

    cleaned_transactions = cleaned[[
        'id', 'timestamp', 'player_id', 'type', 'amount',
    ]]
    cleaned_transactions['id'] = pd.to_numeric(
        cleaned_transactions['id'],
        errors='coerce',
    ).astype('Int64')
    cleaned_transactions['player_id'] = pd.to_numeric(
        cleaned_transactions['player_id'], errors='coerce',
    ).astype('Int64')
    cleaned_transactions['timestamp'] = pd.to_datetime(
        cleaned_transactions['timestamp'], utc=True, errors='coerce',
    )
    cleaned_transactions['amount'] = pd.to_numeric(
        cleaned_transactions['amount'], errors='coerce',
    )

    return write_parquet_file(cleaned_transactions, _transformed_path(TRANSACTIONS_FILE))


@task
def load_parquet_to_postgres(
    filepath: str,
    table_name: str,
    column_order: list[str],
) -> NoReturn:
    """Load parquet data into PostgreSQL table.

    Parameters
    ----------
    filepath : str
        Path to parquet file to load.
    table_name : str
        Name of PostgreSQL table to load data into.
    column_order : list[str]
        Order of columns for the table.
    """
    import io

    from airflow.hooks.base import BaseHook
    import psycopg2

    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    dsn = conn.get_uri()
    frame = read_parquet_file(filepath)[column_order]

    with psycopg2.connect(dsn) as connection:
        with connection.cursor() as cursor:
            buffer = io.StringIO()
            frame.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            columns_str = ', '.join(column_order)
            copy_sql = (
                f"COPY {SCHEMA_NAME}.{table_name} ({columns_str}) FROM STDIN "
                "WITH (FORMAT CSV)"
            )
            cursor.copy_expert(
                sql=copy_sql,
                file=buffer,
            )
        connection.commit()


with DAG(
    dag_id='part_1_task_1_clean_data',
    start_date=datetime(2025, 9, 1),  # noqa: WPS432
    catchup=False,
    schedule_interval=None,
) as dag:
    players_task = prepare_players_dataset()
    affiliates_task = prepare_affiliates_dataset(players_task)
    transactions_task = prepare_transactions_dataset(players_task)

    create_players_table = SQLExecuteQueryOperator(
        task_id='create_raw_players_table',
        conn_id=POSTGRES_CONN_ID,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.raw_players (
            id BIGINT,
            affiliate_id BIGINT,
            country_code VARCHAR(4),
            is_kyc_approved BOOLEAN,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        );
        TRUNCATE TABLE {SCHEMA_NAME}.raw_players;
        """,
    )

    create_affiliates_table = SQLExecuteQueryOperator(
        task_id='create_raw_affiliates_table',
        conn_id=POSTGRES_CONN_ID,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.raw_affiliates (
            id BIGINT,
            code TEXT,
            origin TEXT,
            redeemed_at TIMESTAMP WITH TIME ZONE
        );
        TRUNCATE TABLE {SCHEMA_NAME}.raw_affiliates;
        """,
    )

    create_transactions_table = SQLExecuteQueryOperator(
        task_id='create_raw_transactions_table',
        conn_id=POSTGRES_CONN_ID,
        sql=f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.raw_transactions (
            id BIGINT,
            timestamp TIMESTAMP WITH TIME ZONE,
            player_id BIGINT,
            type TEXT,
            amount NUMERIC
        );
        TRUNCATE TABLE {SCHEMA_NAME}.raw_transactions;
        """,
    )

    load_players = load_parquet_to_postgres.override(
        task_id='load_players_to_postgres',
    )(
        filepath=players_task,
        table_name='raw_players',
        column_order=[
            'id',
            'affiliate_id',
            'country_code',
            'is_kyc_approved',
            'created_at',
            'updated_at',
        ],
    )
    load_affiliates = load_parquet_to_postgres.override(
        task_id='load_affiliates_to_postgres',
    )(
        filepath=affiliates_task, table_name='raw_affiliates', column_order=[
            'id', 'code', 'origin', 'redeemed_at',
        ], )
    load_transactions = load_parquet_to_postgres.override(
        task_id='load_transactions_to_postgres',
    )(
        filepath=transactions_task,
        table_name='raw_transactions',
        column_order=['id', 'timestamp', 'player_id', 'type', 'amount'],
    )

    chain(players_task, create_players_table, load_players)
    chain(players_task, affiliates_task, create_affiliates_table, load_affiliates)
    chain(players_task, transactions_task, create_transactions_table, load_transactions)

"""Generate incremental raw records for players."""
from datetime import datetime, timedelta
from pathlib import Path
import random
import string
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.decorators import task
import pandas as pd

from utils.common import data_root, read_csv_file, write_csv_file


def _expanded_root() -> Path:
    """Create and return the expanded data directory path.

    Returns
    -------
    Path
        Path to the expanded data directory, created if it doesn't exist.
    """
    root = data_root() / 'expanded'
    root.mkdir(parents=True, exist_ok=True)
    return root


def _next_identifier(frame: pd.DataFrame, id_column: str) -> int:
    """Get the next available identifier for a DataFrame column.

    Parameters
    ----------
    frame : pd.DataFrame
        DataFrame to analyze for the next identifier.
    id_column : str
        Name of the column containing identifiers.

    Returns
    -------
    int
        Next available identifier (max + 1, or 1 if empty).
    """
    if frame.empty or frame[id_column].isna().all():
        return 1
    return int(frame[id_column].max()) + 1


def _prepare_random_dttm() -> str:
    """Generate a random datetime within the last 1 year.

    Returns
    -------
    str
        Random timestamp in ISO format with timezone.
    """
    max_days_prior = 365

    end_date = datetime.now().replace(microsecond=0)
    start_date = end_date - timedelta(days=max_days_prior)

    random_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
    random_timestamp = start_date + timedelta(seconds=random_seconds)

    return f"{random_timestamp.isoformat(sep=' ')}+00:00"


def _prepare_random_dttm_after(after_date_str: str) -> str:
    """Generate a random datetime after a given date.

    Parameters
    ----------
    after_date_str : str
        ISO format date string that the random date should be after.

    Returns
    -------
    str
        Random timestamp in ISO format with timezone, guaranteed to be after given date.
    """
    after_date_clean = after_date_str.replace('+00:00', '')
    after_date = datetime.fromisoformat(after_date_clean)

    end_date = datetime.now().replace(microsecond=0)

    if after_date > end_date:
        end_date = after_date + timedelta(days=30)  # noqa: WPS432

    time_diff = end_date - after_date
    if time_diff.total_seconds() <= 0:
        random_timestamp = after_date + timedelta(minutes=random.randint(1, 60))
    else:
        random_seconds = random.randint(0, int(time_diff.total_seconds()))
        random_timestamp = after_date + timedelta(seconds=random_seconds)

    return f"{random_timestamp.isoformat(sep=' ')}+00:00"


def _generate_incremental_affiliate_id(previous_max: int) -> Optional[int]:
    """Generate an incremental affiliate ID or None randomly.

    Parameters
    ----------
    previous_max : int
        The current maximum affiliate ID.

    Returns
    -------
    Optional[int]
        Either previous_max + 1 or None with equal probability.
    """
    return random.choice([previous_max + 1, None])


def _increment_from_prev(prev_value: int) -> int:
    """Increment a value by one.

    Parameters
    ----------
    prev_value : int
        The previous value to increment.

    Returns
    -------
    int
        The incremented value (prev_value + 1).
    """
    return prev_value + 1


def _generate_player_rows(
    qty_to_create: int,
    start_id: int,
    max_affiliate_id: int,
) -> List[Dict]:
    """Generate synthetic player records with random data.

    Parameters
    ----------
    qty_to_create : int
        Number of player records to generate.
    start_id : int
        Starting ID for the new player records.
    max_affiliate_id : int
        Maximum affiliate ID for generating affiliate relationships.

    Returns
    -------
    List[Dict]
        List of dictionaries containing player record data.
    """
    player_id = start_id - 1
    rows: List[Dict] = []
    countries: List[str] = ['DE', 'BR', 'GB', 'US', 'CA']
    affiliate_id_num: int = max_affiliate_id
    for offset in range(qty_to_create):
        player_id = _increment_from_prev(player_id)
        upd_dttm_iso = _prepare_random_dttm()
        country = random.choice(countries)
        affiliate_id = _generate_incremental_affiliate_id(affiliate_id_num)
        if affiliate_id is not None:
            affiliate_id_num = affiliate_id
        kyc_flag = random.choice([True, False])
        rows.append(
            {
                'id': player_id,
                'affiliate_id': affiliate_id,
                'country_code': country,
                'is_kyc_approved': kyc_flag,
                'created_at': upd_dttm_iso,
                'updated_at': upd_dttm_iso,
            },
        )
    return rows


@task
def generate_players_increment() -> str:
    """Append new players to the expanded dataset.

    Returns
    -------
    str
        Path to the expanded players dataset file.
    """
    root = _expanded_root()
    increment_path = root / 'players.csv'

    base_raw_path = data_root() / 'raw' / 'players.csv'
    base_players = read_csv_file(base_raw_path)

    start_id = _next_identifier(base_players, 'id')

    qty_to_create = 1000 - len(base_players)
    if qty_to_create > 0:
        increment_df = pd.DataFrame(
            _generate_player_rows(
                qty_to_create=qty_to_create,
                start_id=start_id,
                max_affiliate_id=base_players['affiliate_id'].max(),
            ),
        )

        expanded_players = pd.concat([base_players, increment_df], ignore_index=True)

        return write_csv_file(frame=expanded_players, path=increment_path)
    return write_csv_file(frame=base_players, path=increment_path)


def _safe_get_from_array(array: pd.Series, index: int, default: str) -> Any:
    """Safely get an element from a pandas Series with a default fallback.

    Parameters
    ----------
    array : pd.Series
        Series to get the element from.
    index : int
        Index position to retrieve.
    default : str
        Default value to return if index is out of bounds.

    Returns
    -------
    Any
        Element at the given index or the default value.
    """
    try:
        return array[index]
    except IndexError:
        return default


def _get_unique_code(array_codes: pd.Series) -> str:
    """Generate a unique uppercase alphabetic code not in the given series.

    Parameters
    ----------
    array_codes : pd.Series
        Series containing existing codes to avoid duplicates.

    Returns
    -------
    str
        A unique uppercase alphabetic code.
    """
    code = ''.join(random.choices(string.ascii_uppercase, k=6))
    while True:
        if code not in array_codes:
            return code
        code += random.choices(string.ascii_uppercase, k=1)


def _generate_affiliates_rows(
    qty_to_create: int,
    increment_df: pd.DataFrame,
    used_codes: List[str],
    start_id: int,
) -> List[Dict]:
    """Generate synthetic affiliate records based on player data.

    Parameters
    ----------
    qty_to_create : int
        Number of affiliate records to generate.
    increment_df : pd.DataFrame
        DataFrame containing player data for affiliate relationships.
    used_codes : List[str]
        List of already used affiliate codes to avoid duplicates.
    start_id : int
        Starting ID for the new affiliate records.

    Returns
    -------
    List[Dict]
        List of dictionaries containing affiliate record data.
    """
    increment_df = increment_df[increment_df['id'] >= start_id]
    ids = increment_df['id'].values
    redeemed_ats = increment_df['redeemed_at'].values
    origins: List[Optional[str]] = ['YouTube', 'Discord', 'X', None]
    id: int = start_id
    rows: List[Dict] = []
    for indx in range(qty_to_create):
        id = _safe_get_from_array(
            array=ids,
            index=indx,
            default=_increment_from_prev(id),
        )
        code = _get_unique_code(array_codes=used_codes)
        used_codes.append(code)
        origin = random.choice(origins)
        redeemed_at = _safe_get_from_array(
            array=redeemed_ats,
            index=indx,
            default=None,
        )
        rows.append(
            {
                'id': id,
                'code': code,
                'origin': origin,
                'redeemed_at': redeemed_at,
            },
        )

    return rows


@task
def generate_affiliates_increment(player_path: Path) -> Path:
    """Generate incremental affiliate records based on player data.

    Parameters
    ----------
    player_path : Path
        Path to the players dataset file.

    Returns
    -------
    Path
        Path to the generated affiliates dataset file.
    """
    root = _expanded_root()
    increment_path = root / 'affiliates.csv'
    affiliates_path = data_root() / 'raw' / 'affiliates.csv'
    players_df = read_csv_file(path=player_path)
    affiliates_df = read_csv_file(path=affiliates_path)

    increment_affiliates_df = players_df[~players_df['affiliate_id'].isna()]
    increment_affiliates_df = increment_affiliates_df[[
        'affiliate_id',
        'created_at',
    ]]
    increment_affiliates_df = increment_affiliates_df.rename(
        columns={
            'affiliate_id': 'id',
            'created_at': 'redeemed_at',
        },
    )
    start_id = _next_identifier(affiliates_df, 'id')

    qty_to_create = 1000 - len(affiliates_df)
    if qty_to_create > 0:
        increment_df = pd.DataFrame(
            _generate_affiliates_rows(
                qty_to_create=qty_to_create,
                increment_df=increment_affiliates_df,
                used_codes=affiliates_df['code'].to_list(),
                start_id=start_id,
            ),
        )

        expanded_affiliates = pd.concat([affiliates_df, increment_df], ignore_index=True)

        return write_csv_file(frame=expanded_affiliates, path=increment_path)
    return write_csv_file(frame=affiliates_df, path=increment_path)


def _generate_transaction_rows(
    qty_to_create: int,
    start_id: int,
    kyc_approved_players_data: List[Dict],
) -> List[Dict]:
    """Generate synthetic transaction rows for KYC-approved players only.

    Parameters
    ----------
    qty_to_create : int
        Number of transaction rows to generate.
    start_id : int
        Starting ID for transaction records.
    kyc_approved_players_data : List[Dict]
        List of dictionaries containing player data (id, created_at).

    Returns
    -------
    List[Dict]
        List of transaction record dictionaries.
    """
    transaction_id = start_id
    rows: List[Dict] = []
    transaction_types: List[str] = ['Deposit', 'Withdraw', 'Bonus']

    min_deposit_amount = 50.0
    max_deposit_amount = 1000.0
    min_withdraw_amount = 25.0
    max_withdraw_amount = 800.0
    min_bonus_amount = 10.0
    max_bonus_amount = 100.0

    for _ in range(qty_to_create):
        if not kyc_approved_players_data:
            break

        player_data = random.choice(kyc_approved_players_data)
        player_id = player_data['id']
        player_created_at = player_data['created_at']

        transaction_type = random.choice(transaction_types)

        if transaction_type == 'Deposit':
            amount = round(random.uniform(min_deposit_amount, max_deposit_amount), 2)
        elif transaction_type == 'Withdraw':
            amount = round(random.uniform(min_withdraw_amount, max_withdraw_amount), 2)
        else:
            amount = round(random.uniform(min_bonus_amount, max_bonus_amount), 2)

        timestamp_iso = _prepare_random_dttm_after(player_created_at)

        rows.append({
            'id': transaction_id,
            'timestamp': timestamp_iso,
            'player_id': player_id,
            'type': transaction_type,
            'amount': amount,
        })

        transaction_id += 1

    return rows


@task
def generate_transactions_increment(players_path: str) -> str:
    """Append new transactions to the expanded dataset, respecting KYC rules.

    Parameters
    ----------
    players_path : str
        Path to the expanded players dataset file.

    Returns
    -------
    str
        Path to the expanded transactions dataset file.
    """
    root = _expanded_root()
    increment_path = root / 'transactions.csv'

    base_raw_path = data_root() / 'raw' / 'transactions.csv'
    base_transactions = read_csv_file(base_raw_path)

    players_df = read_csv_file(players_path)
    kyc_approved_df = players_df[players_df['is_kyc_approved']]
    kyc_approved_players_data = kyc_approved_df[['id', 'created_at']].to_dict('records')
    start_id = _next_identifier(base_transactions, 'id')

    qty_to_create = 1000 - len(base_transactions)
    if qty_to_create > 0 and kyc_approved_players_data:
        increment_df = pd.DataFrame(
            _generate_transaction_rows(
                qty_to_create=qty_to_create,
                start_id=start_id,
                kyc_approved_players_data=kyc_approved_players_data,
            ),
        )

        expanded_transactions = pd.concat(
            [base_transactions, increment_df], ignore_index=True,
        )

        return write_csv_file(frame=expanded_transactions, path=increment_path)
    return write_csv_file(frame=base_transactions, path=increment_path)


with DAG(
    dag_id='part_1_task_2_extend_data',
    start_date=datetime(2025, 9, 1),  # noqa: WPS432
    catchup=False,
    schedule_interval=None,
    is_paused_upon_creation=False,
) as dag:
    generate_players_task = generate_players_increment()
    generate_affiliates_task = generate_affiliates_increment(generate_players_task)
    generate_transactions_task = generate_transactions_increment(generate_players_task)

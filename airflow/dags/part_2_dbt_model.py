"""Airflow DAG that runs DBT models for analytics tables."""

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain

# DBT project path
DBT_PROJECT_PATH = '/opt/dbt'


def _get_dbt_command(command: str) -> str:
    return f"cd {DBT_PROJECT_PATH} && dbt {command}"


with DAG(
    dag_id='part_2_dbt_model',
    start_date=datetime(2025, 9, 1),
    catchup=False,
    schedule_interval=None,
    description='Run DBT models to create analytics tables',
    tags=['dbt', 'analytics'],
    is_paused_upon_creation=False,
) as dag:

    # Run DBT models
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=_get_dbt_command('run'),
    )

    # Test DBT models
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=_get_dbt_command('test'),
    )

    # todo: check docs quality
    # Generate DBT docs
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=_get_dbt_command('docs generate'),
    )

    # Define task dependencies
    chain(
        dbt_run,
        dbt_test,
        dbt_docs_generate,
    )

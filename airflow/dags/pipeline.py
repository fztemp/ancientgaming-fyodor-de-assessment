"""
This is a placeholder Python file to be used to create the DAG which schedule the DBT process.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    'pipeline',
    start_date=datetime(2025, 9, 1),
    catchup=False,
) as dag:
    t1 = EmptyOperator(task_id='placeholder_task')

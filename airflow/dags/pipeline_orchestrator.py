"""Complete pipeline DAG that orchestrates data generation, modeling, and analytics."""
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

POKE_INTERVAL = 5

with DAG(
    dag_id='pipeline_orchestrator',
    start_date=datetime(2025, 9, 1),  # noqa: WPS432
    catchup=False,
    schedule_interval='@daily',
    description='Complete data pipeline from generation to analytics',
    tags=['pipeline', 'complete'],
    is_paused_upon_creation=False,
) as dag:

    # Step 1: Generate expanded datasets
    trigger_data_generation = TriggerDagRunOperator(
        task_id='trigger_data_generation',
        trigger_dag_id='part_1_task_2_extend_data',
        wait_for_completion=True,
        poke_interval=POKE_INTERVAL,
    )

    # Step 2: Wait for data modeling to complete
    trigger_data_modeling = TriggerDagRunOperator(
        task_id='trigger_data_modeling',
        trigger_dag_id='part_1_task_1_clean_data',
        wait_for_completion=True,
        poke_interval=POKE_INTERVAL,
    )

    # Step 3: Run DBT analytics
    trigger_dbt_analytics = TriggerDagRunOperator(
        task_id='trigger_dbt_analytics',
        trigger_dag_id='part_2_dbt_model',
        wait_for_completion=True,
        poke_interval=POKE_INTERVAL,
    )

    # Define pipeline flow
    chain(
        trigger_data_generation,
        trigger_data_modeling,
        trigger_dbt_analytics,
    )

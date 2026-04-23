"""Airflow DAG — CDC Merge (Application Silver).

Resolves Bronze CDC events into Silver current-state tables.
One task per entity — all four run in parallel.

DAG ordering: Runs after bronze_ingest.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "eam-poc",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cdc_merge_application",
    description="Resolve Bronze CDC into Silver application current-state tables",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["eam", "silver", "cdc-merge"],
) as dag:
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="bronze_ingest",
        external_task_id="ingest_kafka_to_bronze",
        mode="poke",
        timeout=300,
        poke_interval=10,
    )

    # One task per entity — these can run in parallel
    merge_asset = BashOperator(
        task_id="merge_asset",
        bash_command="python -m transforms.polars.app.merge_asset",
    )

    merge_work_request = BashOperator(
        task_id="merge_work_request",
        bash_command="python -m transforms.polars.app.merge_work_request",
    )

    merge_work_order = BashOperator(
        task_id="merge_work_order",
        bash_command="python -m transforms.polars.app.merge_work_order",
    )

    merge_maintenance_action = BashOperator(
        task_id="merge_maintenance_action",
        bash_command="python -m transforms.polars.app.merge_maintenance_action",
    )

    # All merges depend on Bronze being ready, but are independent of each other
    wait_for_bronze >> [merge_asset, merge_work_request, merge_work_order, merge_maintenance_action]

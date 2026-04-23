"""Airflow DAG — Gold Rollups.

Computes analytics and reporting metrics from Silver and Silver-S5000F layers.
All Gold tasks can run in parallel.

DAG ordering: Runs after s5000f_transform (final DAG in the pipeline).
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
    dag_id="gold_rollups",
    description="Compute Gold analytics from Silver and Silver-S5000F layers",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["eam", "gold", "analytics"],
) as dag:
    wait_for_s5000f = ExternalTaskSensor(
        task_id="wait_for_s5000f",
        external_dag_id="s5000f_transform",
        external_task_id="transform_maintenance_event",
        mode="poke",
        timeout=300,
        poke_interval=10,
    )

    # All Gold rollups can run in parallel
    asset_availability = BashOperator(
        task_id="asset_availability",
        bash_command="python -m transforms.polars.gold.asset_availability",
    )

    work_order_backlog = BashOperator(
        task_id="work_order_backlog",
        bash_command="python -m transforms.polars.gold.work_order_backlog",
    )

    maintenance_history = BashOperator(
        task_id="maintenance_history",
        bash_command="python -m transforms.polars.gold.maintenance_history",
    )

    mtbf_metrics = BashOperator(
        task_id="mtbf_metrics",
        bash_command="python -m transforms.polars.gold.mtbf_metrics",
    )

    wait_for_s5000f >> [asset_availability, work_order_backlog, maintenance_history, mtbf_metrics]

"""Airflow DAG — S5000F Semantic Transformation.

Transforms Silver application tables into Silver-S5000F aligned tables.
ProductInstance runs first (others depend on it for ID references).

DAG ordering: Runs after cdc_merge_application.
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
    dag_id="s5000f_transform",
    description="Transform Silver application data into S5000F-aligned structures",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["eam", "s5000f", "semantic-transform"],
) as dag:
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="cdc_merge_application",
        # Wait for all merge tasks — use the last one as a proxy
        external_task_id="merge_asset",
        mode="poke",
        timeout=300,
        poke_interval=10,
    )

    # ProductInstance must run first (other concepts reference it)
    product_instance = BashOperator(
        task_id="transform_product_instance",
        bash_command="python -m transforms.polars.s5000f.product_instance",
    )

    # These can run in parallel after ProductInstance
    functional_failure = BashOperator(
        task_id="transform_functional_failure",
        bash_command="python -m transforms.polars.s5000f.functional_failure",
    )

    maintenance_task = BashOperator(
        task_id="transform_maintenance_task",
        bash_command="python -m transforms.polars.s5000f.maintenance_task",
    )

    maintenance_task_step = BashOperator(
        task_id="transform_maintenance_task_step",
        bash_command="python -m transforms.polars.s5000f.maintenance_task_step",
    )

    # MaintenanceEvent depends on MaintenanceTask (for task IDs)
    maintenance_event = BashOperator(
        task_id="transform_maintenance_event",
        bash_command="python -m transforms.polars.s5000f.maintenance_event",
    )

    # Dependency graph
    wait_for_silver >> product_instance
    product_instance >> [functional_failure, maintenance_task, maintenance_task_step]
    maintenance_task >> maintenance_event

"""Airflow DAG — EAM Simulator.

Triggers the EAM simulator to produce CDC events to Kafka.
Parameterised by batch size. No business logic in the DAG.

DAG ordering: This is the first DAG in the pipeline.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "eam-poc",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="eam_simulator",
    description="Generate simulated EAM CDC events and produce to Kafka",
    default_args=default_args,
    schedule=None,  # Manually triggered or sensor-triggered
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["eam", "simulator", "cdc"],
    params={"batch_size": 100, "seed": 42},
) as dag:
    produce_events = BashOperator(
        task_id="produce_cdc_events",
        bash_command=(
            "python -m eam_simulator.produce_cdc "
            "--events {{ params.batch_size }} "
            "--seed {{ params.seed }}"
        ),
    )

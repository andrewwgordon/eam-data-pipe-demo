"""Airflow DAG — Bronze Ingestion.

Consumes CDC events from Kafka and writes to Iceberg Bronze tables.
Parameterised by poll timeout.

DAG ordering: Runs after eam_simulator.
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
    dag_id="bronze_ingest",
    description="Ingest CDC events from Kafka into Iceberg Bronze tables",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["eam", "bronze", "ingestion"],
    params={"kafka_timeout": 15},
) as dag:
    wait_for_simulator = ExternalTaskSensor(
        task_id="wait_for_simulator",
        external_dag_id="eam_simulator",
        external_task_id="produce_cdc_events",
        mode="poke",
        timeout=300,
        poke_interval=10,
    )

    ingest = BashOperator(
        task_id="ingest_kafka_to_bronze",
        bash_command=(
            "python -m transforms.polars.app.bronze_ingest "
            "--timeout {{ params.kafka_timeout }}"
        ),
    )

    wait_for_simulator >> ingest

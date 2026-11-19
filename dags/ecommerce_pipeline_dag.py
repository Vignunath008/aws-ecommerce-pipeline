"""
Airflow DAG: ecommerce_pipeline   (Airflow 2.7+, apache-airflow-providers-amazon)

S3 sensor (validated/<ds>/) -> Glue transform -> Redshift load -> data quality checks

This DAG is the ONLY thing that starts the Glue job. The Lambda just validates and
routes files into validated/<arrival-date>/, so each day's data is processed once.
Connections: aws_default, redshift_default
"""
import os
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

RAW_BUCKET = os.getenv("RAW_BUCKET", "ecom-raw")
CURATED_PATH = os.getenv("CURATED_PATH", "s3://ecom-curated/orders/")   # must end with /
REDSHIFT_IAM_ROLE = os.getenv("REDSHIFT_IAM_ROLE", "arn:aws:iam::123456789012:role/redshift-s3-read")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "transform_orders")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_DIR = os.path.join(BASE_DIR, "..", "sql")
QUALITY_FILE = os.path.join(BASE_DIR, "..", "quality", "checks.sql")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def parse_checks(text):
    """Split checks.sql into [(name, sql), ...] using '-- name:' markers."""
    checks = []
    for block in re.split(r"^--\s*name:\s*", text, flags=re.MULTILINE)[1:]:
        header, _, body = block.partition("\n")
        name = header.split()[0]
        sql = "\n".join(l for l in body.splitlines() if not l.strip().startswith("--"))
        sql = sql.strip().rstrip(";").strip()
        if sql:
            checks.append((name, sql))
    return checks


def run_quality_checks(**_):
    with open(QUALITY_FILE) as f:
        checks = parse_checks(f.read())
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")
    failures = []
    for name, sql in checks:
        row = hook.get_first(sql)
        value = row[0] if row else None
        print(f"[DQ] {name}: {value}")
        if value is None or value > 0:
            failures.append(f"{name}={value}")
    if failures:
        raise AirflowException("Data quality checks failed: " + ", ".join(failures))


with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath=[SQL_DIR],
    tags=["ecommerce", "aws"],
) as dag:

    # {{ ds }} = the day being processed (files that ARRIVED that day)
    wait_for_file = S3KeySensor(
        task_id="wait_for_orders_file",
        bucket_name=RAW_BUCKET,
        bucket_key="validated/{{ ds }}/*.csv",
        wildcard_match=True,
        poke_interval=300,
        timeout=6 * 60 * 60,
        mode="reschedule",
        soft_fail=True,          # no files that day -> skip instead of fail
    )

    transform = GlueJobOperator(
        task_id="glue_transform_orders",
        job_name=GLUE_JOB_NAME,
        script_args={
            "--source_path": "s3://" + RAW_BUCKET + "/validated/{{ ds }}/",
            "--target_path": CURATED_PATH + "load_date={{ ds }}/",
        },
        wait_for_completion=True,
    )

    load = SQLExecuteQueryOperator(
        task_id="load_redshift",
        conn_id="redshift_default",
        sql="load_redshift.sql",
        split_statements=True,
        autocommit=False,        # whole file commits as one transaction
        params={
            "curated_path": CURATED_PATH,
            "iam_role": REDSHIFT_IAM_ROLE,
        },
    )

    quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_quality_checks,
    )

    wait_for_file >> transform >> load >> quality

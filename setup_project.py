#!/usr/bin/env python3
"""
setup_project.py  (v3)
Generates the AWS e-commerce data pipeline project folder and a zip of it:
code, Terraform, IAM policies, tests, sample data, README diagrams and sample output.

Usage:
    python setup_project.py            # creates ./aws-ecommerce-pipeline and aws-ecommerce-pipeline.zip
    python setup_project.py my_folder  # custom folder name
"""
import os
import sys
import zipfile

PROJECT = sys.argv[1] if len(sys.argv) > 1 else "aws-ecommerce-pipeline"

FILES = {}

FILES['lambda/validate_and_trigger.py'] = r'''"""
Lambda: validate_and_trigger
Triggered by S3 ObjectCreated on s3://<raw-bucket>/incoming/ (suffix .csv).

- Validates extension, size and CSV header.
- Good files are moved to  validated/<arrival-date>/<file>.csv
  -> the Airflow DAG's S3 sensor picks them up for that date (single trigger path,
     so Glue never runs twice on the same file).
- Bad files are moved to   quarantine/<arrival-date>/<file>  with a reject reason.
"""
import csv
import io
import json
import logging
import os
import urllib.parse
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

VALIDATED_PREFIX = os.environ.get("VALIDATED_PREFIX", "validated/")
QUARANTINE_PREFIX = os.environ.get("QUARANTINE_PREFIX", "quarantine/")
MAX_BYTES = int(os.environ.get("MAX_BYTES", str(500 * 1024 * 1024)))

REQUIRED_COLUMNS = [
    "order_id", "order_ts", "customer_id", "product_id",
    "quantity", "unit_price", "currency", "order_status",
]


def validate_header(first_line):
    """Return an error string, or None if the header has every required column."""
    if not first_line.strip():
        return "missing header row"
    header = [c.strip().lower().lstrip("﻿") for c in next(csv.reader(io.StringIO(first_line)))]
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    return f"missing columns: {missing}" if missing else None


def arrival_date(record):
    """YYYY-MM-DD from the S3 event time (UTC); falls back to now."""
    ts = record.get("eventTime")
    if ts:
        return ts[:10]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def check_object(bucket, key):
    if not key.lower().endswith(".csv"):
        return "not a .csv file"
    size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    if size == 0:
        return "empty file"
    if size > MAX_BYTES:
        return f"file too large ({size} bytes)"
    body = s3.get_object(Bucket=bucket, Key=key, Range="bytes=0-4095")["Body"].read()
    lines = body.decode("utf-8", errors="replace").splitlines()
    return validate_header(lines[0] if lines else "")


def move(bucket, src_key, dest_key, metadata=None):
    kwargs = {"Bucket": bucket, "CopySource": {"Bucket": bucket, "Key": src_key}, "Key": dest_key}
    if metadata:
        kwargs.update(Metadata=metadata, MetadataDirective="REPLACE")
    s3.copy_object(**kwargs)
    s3.delete_object(Bucket=bucket, Key=src_key)


def lambda_handler(event, context):
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        file_name = key.split("/")[-1]
        day = arrival_date(record)
        logger.info("Received s3://%s/%s", bucket, key)

        try:
            error = check_object(bucket, key)
        except Exception as exc:  # unreadable object etc.
            error = f"validation error: {exc}"

        if error:
            dest = f"{QUARANTINE_PREFIX}{day}/{file_name}"
            move(bucket, key, dest, metadata={"reject_reason": error[:1024]})
            logger.warning("Quarantined %s -> %s (%s)", key, dest, error)
            results.append({"status": "quarantined", "key": key, "dest": dest, "reason": error})
        else:
            dest = f"{VALIDATED_PREFIX}{day}/{file_name}"
            move(bucket, key, dest)
            logger.info("Validated %s -> %s", key, dest)
            results.append({"status": "validated", "key": key, "dest": dest})

    return {"statusCode": 200, "body": json.dumps(results)}
'''

FILES['glue/transform_orders.py'] = r'''"""
Glue 4.0 PySpark job: transform_orders
Reads one day's validated CSVs, cleans/types/dedupes them, and OVERWRITES
s3://<curated>/orders/load_date=<ds>/ with Parquet (idempotent re-runs).

Job args: --source_path  --target_path
The transform() function is pure PySpark so it can be unit-tested locally.
"""
import sys

# Column order MUST match staging.orders_stg (Redshift Parquet COPY maps by position)
STAGING_COLUMNS = [
    "order_id", "order_ts", "customer_id", "product_id", "quantity",
    "unit_price", "currency", "order_status", "line_total", "source_file", "load_ts",
]


def transform(raw):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df = raw.toDF(*[c.strip().lower().lstrip("﻿") for c in raw.columns])
    if "source_file" not in df.columns:
        df = df.withColumn("source_file", F.lit(None).cast("string"))

    df = (df
          .withColumn("order_id", F.trim("order_id"))
          .withColumn("customer_id", F.trim("customer_id"))
          .withColumn("product_id", F.trim("product_id"))
          # try_cast -> NULL on bad values instead of failing (safe with ANSI mode on/off)
          .withColumn("order_ts", F.expr("try_cast(trim(order_ts) AS TIMESTAMP)"))
          .withColumn("quantity", F.expr("try_cast(trim(quantity) AS INT)"))
          .withColumn("unit_price", F.expr("try_cast(trim(unit_price) AS DECIMAL(12,2))"))
          .withColumn("currency", F.upper(F.trim("currency")))
          .withColumn("order_status", F.lower(F.trim("order_status")))
          .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
          .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
          .filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))
          .filter(F.col("order_ts").isNotNull())
          .filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
          .filter(F.col("unit_price").isNotNull() & (F.col("unit_price") >= 0)))

    # Keep the latest version of each order line (ties broken by file name)
    w = (Window.partitionBy("order_id", "product_id")
               .orderBy(F.col("order_ts").desc(), F.col("source_file").desc_nulls_last()))
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    df = (df
          .withColumn("line_total", (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)"))
          .withColumn("load_ts", F.current_timestamp()))

    return df.select(*STAGING_COLUMNS)


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "target_path"])

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    raw = (spark.read
           .option("header", "true")
           .option("mode", "PERMISSIVE")
           .csv(args["source_path"])
           .withColumn("source_file", F.input_file_name()))

    out = transform(raw).cache()
    row_count = out.count()          # count once, before writing
    if row_count == 0:
        raise ValueError(f"No valid rows found in {args['source_path']}")

    out.write.mode("overwrite").parquet(args["target_path"])
    print(f"Wrote {row_count} rows to {args['target_path']}")
    job.commit()


if __name__ == "__main__":
    main()
'''

FILES['sql/load_redshift.sql'] = r'''-- Load one day's curated Parquet into Redshift and upsert into analytics tables.
-- Rendered by Airflow (Jinja): {{ params.curated_path }}, {{ params.iam_role }}, {{ ds }}.
-- Airflow runs these statements in ONE transaction (split_statements=True, autocommit=False).
-- DELETE is used instead of TRUNCATE because TRUNCATE commits implicitly in Redshift.

DELETE FROM staging.orders_stg;

COPY staging.orders_stg
FROM '{{ params.curated_path }}load_date={{ ds }}/'
IAM_ROLE '{{ params.iam_role }}'
FORMAT AS PARQUET;

-- Deduplicate staging (protects the fact table even if a batch has repeats)
DROP TABLE IF EXISTS orders_dedup;

CREATE TEMP TABLE orders_dedup AS
SELECT order_id, order_ts, customer_id, product_id, quantity, unit_price,
       currency, order_status, line_total, load_ts
FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY order_id, product_id
                              ORDER BY order_ts DESC, load_ts DESC) AS rn
    FROM staging.orders_stg s
) x
WHERE rn = 1;

-- New customers
INSERT INTO analytics.dim_customer (customer_id, first_seen_ts)
SELECT s.customer_id, MIN(s.order_ts)
FROM orders_dedup s
LEFT JOIN analytics.dim_customer d ON d.customer_id = s.customer_id
WHERE d.customer_id IS NULL
GROUP BY s.customer_id;

-- New products
INSERT INTO analytics.dim_product (product_id, first_seen_ts)
SELECT s.product_id, MIN(s.order_ts)
FROM orders_dedup s
LEFT JOIN analytics.dim_product d ON d.product_id = s.product_id
WHERE d.product_id IS NULL
GROUP BY s.product_id;

-- Upsert fact rows (delete + insert)
DELETE FROM analytics.fact_order_lines
USING orders_dedup s
WHERE analytics.fact_order_lines.order_id = s.order_id
  AND analytics.fact_order_lines.product_id = s.product_id;

INSERT INTO analytics.fact_order_lines (
    order_id, product_id, customer_id, order_ts, order_date,
    quantity, unit_price, line_total, currency, order_status, load_ts
)
SELECT order_id, product_id, customer_id, order_ts, TRUNC(order_ts),
       quantity, unit_price, line_total, currency, order_status, load_ts
FROM orders_dedup;
'''

FILES['sql/redshift_schema.sql'] = r'''-- Redshift schema for the e-commerce pipeline
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Staging table: column order matches glue/transform_orders.py STAGING_COLUMNS
DROP TABLE IF EXISTS staging.orders_stg;
CREATE TABLE staging.orders_stg (
    order_id      VARCHAR(64),
    order_ts      TIMESTAMP,
    customer_id   VARCHAR(64),
    product_id    VARCHAR(64),
    quantity      INTEGER,
    unit_price    DECIMAL(12,2),
    currency      VARCHAR(3),
    order_status  VARCHAR(32),
    line_total    DECIMAL(14,2),
    source_file   VARCHAR(1024),
    load_ts       TIMESTAMP
);

-- Dimensions
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key  BIGINT IDENTITY(1,1),
    customer_id   VARCHAR(64) NOT NULL,
    first_seen_ts TIMESTAMP,
    PRIMARY KEY (customer_key)
) DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key   BIGINT IDENTITY(1,1),
    product_id    VARCHAR(64) NOT NULL,
    first_seen_ts TIMESTAMP,
    PRIMARY KEY (product_key)
) DISTSTYLE ALL;

-- Fact table
CREATE TABLE IF NOT EXISTS analytics.fact_order_lines (
    order_id      VARCHAR(64)   NOT NULL,
    product_id    VARCHAR(64)   NOT NULL,
    customer_id   VARCHAR(64)   NOT NULL,
    order_ts      TIMESTAMP     NOT NULL,
    order_date    DATE          NOT NULL,
    quantity      INTEGER       NOT NULL,
    unit_price    DECIMAL(12,2) NOT NULL,
    line_total    DECIMAL(14,2) NOT NULL,
    currency      VARCHAR(3),
    order_status  VARCHAR(32),
    load_ts       TIMESTAMP,
    PRIMARY KEY (order_id, product_id)
)
DISTKEY (customer_id)
SORTKEY (order_date);
'''

FILES['quality/checks.sql'] = r'''-- Data quality checks. Each block starts with "-- name: <check_name>" and
-- returns ONE number = count of failing rows. The DAG fails if any value > 0.

-- name: staging_not_empty
SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM staging.orders_stg;

-- name: null_keys
SELECT COUNT(*) FROM analytics.fact_order_lines
WHERE order_id IS NULL OR product_id IS NULL OR customer_id IS NULL;

-- name: duplicate_order_lines
SELECT COUNT(*) FROM (
    SELECT order_id, product_id FROM analytics.fact_order_lines
    GROUP BY order_id, product_id HAVING COUNT(*) > 1
) d;

-- name: non_positive_quantity
SELECT COUNT(*) FROM analytics.fact_order_lines WHERE quantity <= 0;

-- name: line_total_mismatch
SELECT COUNT(*) FROM analytics.fact_order_lines
WHERE ABS(line_total - quantity * unit_price) > 0.01;

-- name: future_orders
SELECT COUNT(*) FROM analytics.fact_order_lines WHERE order_ts > DATEADD(day, 1, GETDATE());

-- name: orphan_customers
SELECT COUNT(*) FROM analytics.fact_order_lines f
LEFT JOIN analytics.dim_customer c ON c.customer_id = f.customer_id
WHERE c.customer_id IS NULL;

-- name: orphan_products
SELECT COUNT(*) FROM analytics.fact_order_lines f
LEFT JOIN analytics.dim_product p ON p.product_id = f.product_id
WHERE p.product_id IS NULL;
'''

FILES['dags/ecommerce_pipeline_dag.py'] = r'''"""
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
'''

FILES['iam/airflow_policy.json'] = r'''{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SenseValidatedFiles",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::${raw_bucket}"
    },
    {
      "Sid": "RunGlueJob",
      "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:GetJob"],
      "Resource": "arn:aws:glue:*:*:job/${glue_job_name}"
    },
    {
      "Sid": "GlueLogs",
      "Effect": "Allow",
      "Action": ["logs:GetLogEvents", "logs:DescribeLogStreams", "logs:FilterLogEvents"],
      "Resource": "arn:aws:logs:*:*:log-group:/aws-glue/*"
    }
  ]
}
'''

FILES['iam/glue_policy.json'] = r'''{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBuckets",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${raw_bucket}",
        "arn:aws:s3:::${curated_bucket}",
        "arn:aws:s3:::${scripts_bucket}"
      ]
    },
    {
      "Sid": "ReadValidatedAndScripts",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::${raw_bucket}/validated/*",
        "arn:aws:s3:::${scripts_bucket}/*"
      ]
    },
    {
      "Sid": "WriteCurated",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::${curated_bucket}/*"
    },
    {
      "Sid": "GlueTempDir",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::${scripts_bucket}/tmp/*"
    }
  ]
}
'''

FILES['iam/lambda_policy.json'] = r'''{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadAndDeleteIncoming",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::${raw_bucket}/incoming/*"
    },
    {
      "Sid": "WriteValidatedAndQuarantine",
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": [
        "arn:aws:s3:::${raw_bucket}/validated/*",
        "arn:aws:s3:::${raw_bucket}/quarantine/*"
      ]
    },
    {
      "Sid": "Logs",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
'''

FILES['iam/redshift_s3_read_policy.json'] = r'''{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${curated_bucket}",
        "arn:aws:s3:::${curated_bucket}/*"
      ]
    }
  ]
}
'''

FILES['terraform/main.tf'] = r'''terraform {
  required_version = ">= 1.5"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.40" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "me" {}

locals {
  prefix         = "${var.project}-${var.env}"
  suffix         = data.aws_caller_identity.me.account_id
  raw_bucket     = "${local.prefix}-raw-${local.suffix}"
  curated_bucket = "${local.prefix}-curated-${local.suffix}"
  scripts_bucket = "${local.prefix}-scripts-${local.suffix}"
  glue_job_name  = "${local.prefix}-transform-orders"
}

# ---------------------------------------------------------------- S3
resource "aws_s3_bucket" "raw" {
  bucket = local.raw_bucket
}

resource "aws_s3_bucket" "curated" {
  bucket = local.curated_bucket
}

resource "aws_s3_bucket" "scripts" {
  bucket = local.scripts_bucket
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    raw     = aws_s3_bucket.raw.id
    curated = aws_s3_bucket.curated.id
    scripts = aws_s3_bucket.scripts.id
  }
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = {
    raw     = aws_s3_bucket.raw.id
    curated = aws_s3_bucket.curated.id
    scripts = aws_s3_bucket.scripts.id
  }
  bucket = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ---------------------------------------------------------------- Lambda
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/validate_and_trigger.py"
  output_path = "${path.module}/build/validate_and_trigger.zip"
}

resource "aws_iam_role" "lambda" {
  name = "${local.prefix}-lambda-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${local.prefix}-lambda-policy"
  role   = aws_iam_role.lambda.id
  policy = templatefile("${path.module}/../iam/lambda_policy.json", { raw_bucket = local.raw_bucket })
}

resource "aws_lambda_function" "validate" {
  function_name    = "${local.prefix}-validate-and-trigger"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "validate_and_trigger.lambda_handler"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 60
  memory_size      = 256
  environment {
    variables = {
      VALIDATED_PREFIX  = "validated/"
      QUARANTINE_PREFIX = "quarantine/"
    }
  }
}

resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.validate.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

resource "aws_s3_bucket_notification" "raw_incoming" {
  bucket = aws_s3_bucket.raw.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.validate.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "incoming/"
    filter_suffix       = ".csv"
  }
  depends_on = [aws_lambda_permission.s3_invoke]
}

# ---------------------------------------------------------------- Glue
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.scripts.id
  key    = "glue/transform_orders.py"
  source = "${path.module}/../glue/transform_orders.py"
  etag   = filemd5("${path.module}/../glue/transform_orders.py")
}

resource "aws_iam_role" "glue" {
  name = "${local.prefix}-glue-role"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue" {
  name = "${local.prefix}-glue-policy"
  role = aws_iam_role.glue.id
  policy = templatefile("${path.module}/../iam/glue_policy.json", {
    raw_bucket     = local.raw_bucket
    curated_bucket = local.curated_bucket
    scripts_bucket = local.scripts_bucket
  })
}

resource "aws_glue_job" "transform_orders" {
  name              = local.glue_job_name
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/${aws_s3_object.glue_script.key}"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.scripts.bucket}/tmp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}

# ---------------------------------------------------------------- Redshift
resource "aws_iam_role" "redshift_s3_read" {
  name = "${local.prefix}-redshift-s3-read"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"] }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3_read" {
  name   = "${local.prefix}-redshift-s3-read"
  role   = aws_iam_role.redshift_s3_read.id
  policy = templatefile("${path.module}/../iam/redshift_s3_read_policy.json", { curated_bucket = local.curated_bucket })
}

resource "aws_redshiftserverless_namespace" "main" {
  count               = var.create_redshift ? 1 : 0
  namespace_name      = "${local.prefix}-ns"
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3_read.arn]
  default_iam_role_arn = aws_iam_role.redshift_s3_read.arn

  lifecycle {
    precondition {
      condition     = var.redshift_admin_password != null
      error_message = "Set redshift_admin_password in terraform.tfvars (or set create_redshift = false)."
    }
  }
}

resource "aws_redshiftserverless_workgroup" "main" {
  count               = var.create_redshift ? 1 : 0
  namespace_name      = aws_redshiftserverless_namespace.main[0].namespace_name
  workgroup_name      = "${local.prefix}-wg"
  base_capacity       = 8
  publicly_accessible = false
}

# ---------------------------------------------------------------- Airflow (MWAA) policy
# Attach this to your MWAA execution role (MWAA itself is not created here).
resource "aws_iam_policy" "airflow" {
  name = "${local.prefix}-airflow-policy"
  policy = templatefile("${path.module}/../iam/airflow_policy.json", {
    raw_bucket    = local.raw_bucket
    glue_job_name = local.glue_job_name
  })
}
'''

FILES['terraform/outputs.tf'] = r'''output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "curated_path" {
  value = "s3://${aws_s3_bucket.curated.bucket}/orders/"
}

output "glue_job_name" {
  value = aws_glue_job.transform_orders.name
}

output "lambda_function_name" {
  value = aws_lambda_function.validate.function_name
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift_s3_read.arn
}

output "redshift_endpoint" {
  value = var.create_redshift ? aws_redshiftserverless_workgroup.main[0].endpoint : null
}

output "airflow_policy_arn" {
  value = aws_iam_policy.airflow.arn
}
'''

FILES['terraform/terraform.tfvars.example'] = r'''aws_region              = "us-east-1"
project                 = "ecom-pipeline"
env                     = "dev"
create_redshift         = true
redshift_admin_password = "ChangeMe123"   # copy this file to terraform.tfvars; never commit real secrets
'''

FILES['terraform/variables.tf'] = r'''variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "ecom-pipeline"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "create_redshift" {
  description = "Create a Redshift Serverless namespace + workgroup (costs money while running)"
  type        = bool
  default     = true
}

variable "redshift_db_name" {
  type    = string
  default = "ecommerce"
}

variable "redshift_admin_user" {
  type    = string
  default = "admin_user"
}

variable "redshift_admin_password" {
  description = "Set in terraform.tfvars (never commit it). 8-64 chars, upper, lower and a digit."
  type        = string
  sensitive   = true
  default     = null
}
'''

FILES['sample_data/bad_orders_missing_column.csv'] = r'''order_id,order_ts,customer_id,product_id,quantity,currency,order_status
O2001,2026-11-18 09:00:00,C010,P100,1,USD,placed
'''

FILES['sample_data/orders_sample.csv'] = r'''order_id,order_ts,customer_id,product_id,quantity,unit_price,currency,order_status
O1001,2026-11-18 09:15:00,C001,P100,2,19.99,usd,PLACED
O1001,2026-11-18 09:15:00,C001,P200,1,5.50,usd,PLACED
O1002,2026-11-18 10:02:30,C002,P100,1,19.99,USD,placed
O1002,2026-11-18 11:45:00,C002,P100,3,19.99,USD,shipped
O1003,2026-11-18 12:30:00,C003,P300,0,49.00,USD,placed
,2026-11-18 13:00:00,C004,P100,1,19.99,USD,placed
O1004,not-a-date,C004,P100,1,19.99,USD,placed
O1005,2026-11-18 14:10:00,C005,P400,1,-3.00,USD,placed
O1006,2026-11-18 15:20:00, C006 ,P500,4,12.25,eur, Delivered
'''

FILES['tests/conftest.py'] = r'''import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# "lambda" is a Python keyword, so add the folders to sys.path instead of importing as packages
for sub in ("lambda", "glue", "dags"):
    sys.path.insert(0, os.path.join(ROOT, sub))

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
'''

FILES['tests/test_dag.py'] = r'''import os

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.amazon")

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def test_dag_loads_and_order():
    from airflow.models import DagBag
    bag = DagBag(dag_folder=os.path.join(ROOT, "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("ecommerce_pipeline")
    assert [t.task_id for t in dag.topological_sort()] == [
        "wait_for_orders_file", "glue_transform_orders", "load_redshift", "data_quality_checks"]


def test_parse_checks():
    import ecommerce_pipeline_dag as d
    with open(os.path.join(ROOT, "quality", "checks.sql")) as f:
        assert len(d.parse_checks(f.read())) == 8
'''

FILES['tests/test_quality_checks.py'] = r'''import os
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _parse(text):
    # same rules as dags/ecommerce_pipeline_dag.py::parse_checks
    out = []
    for block in re.split(r"^--\s*name:\s*", text, flags=re.MULTILINE)[1:]:
        header, _, body = block.partition("\n")
        sql = "\n".join(l for l in body.splitlines() if not l.strip().startswith("--")).strip().rstrip(";")
        out.append((header.split()[0], sql))
    return out


def test_every_check_has_one_select():
    with open(os.path.join(ROOT, "quality", "checks.sql")) as f:
        checks = _parse(f.read())
    assert len(checks) == 8
    for name, sql in checks:
        assert sql.upper().startswith("SELECT"), name
        assert ";" not in sql, name


def test_load_sql_has_no_truncate_or_manual_transaction():
    with open(os.path.join(ROOT, "sql", "load_redshift.sql")) as f:
        code = "\n".join(l for l in f if not l.strip().startswith("--")).upper()
    assert "TRUNCATE" not in code
    assert "BEGIN;" not in code and "COMMIT;" not in code
    assert "LOAD_DATE={{ DS }}/" in code
'''

FILES['tests/test_transform_orders.py'] = r'''import os
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def result(spark):
    from transform_orders import transform
    raw = spark.read.option("header", "true").csv(os.path.join(ROOT, "sample_data", "orders_sample.csv"))
    return {(r.order_id, r.product_id): r for r in transform(raw).collect()}, transform(raw).columns


def test_columns_match_staging_order(result):
    from transform_orders import STAGING_COLUMNS
    assert result[1] == STAGING_COLUMNS


def test_bad_rows_removed(result):
    rows, _ = result
    ids = {k[0] for k in rows}
    assert "O1003" not in ids   # quantity 0
    assert "O1004" not in ids   # bad timestamp
    assert "O1005" not in ids   # negative price
    assert None not in ids and "" not in ids  # missing order_id


def test_dedup_keeps_latest(result):
    rows, _ = result
    r = rows[("O1002", "P100")]
    assert r.quantity == 3 and r.order_status == "shipped"


def test_cleaning_and_line_total(result):
    rows, _ = result
    assert len(rows) == 4
    r = rows[("O1006", "P500")]
    assert r.customer_id == "C006"
    assert r.currency == "EUR"
    assert r.order_status == "delivered"
    assert r.line_total == Decimal("49.00")
'''

FILES['tests/test_validate_lambda.py'] = r'''import os

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BUCKET = "test-raw"


def _event(key, when="2026-11-18T10:00:00.000Z"):
    return {"Records": [{"eventTime": when,
                         "s3": {"bucket": {"name": BUCKET}, "object": {"key": key}}}]}


@pytest.fixture
def lam():
    with moto.mock_aws():
        import importlib
        import validate_and_trigger as mod
        importlib.reload(mod)            # create the boto3 client inside the mock
        mod.s3.create_bucket(Bucket=BUCKET)
        yield mod


def _keys(mod):
    resp = mod.s3.list_objects_v2(Bucket=BUCKET)
    return sorted(o["Key"] for o in resp.get("Contents", []))


def test_validate_header_ok():
    import validate_and_trigger as mod
    assert mod.validate_header(",".join(mod.REQUIRED_COLUMNS)) is None


def test_validate_header_missing():
    import validate_and_trigger as mod
    err = mod.validate_header("order_id,order_ts")
    assert err and "customer_id" in err


def test_good_file_goes_to_validated(lam):
    with open(os.path.join(ROOT, "sample_data", "orders_sample.csv"), "rb") as f:
        lam.s3.put_object(Bucket=BUCKET, Key="incoming/orders_1.csv", Body=f.read())
    out = lam.lambda_handler(_event("incoming/orders_1.csv"), None)
    assert out["statusCode"] == 200
    assert _keys(lam) == ["validated/2026-11-18/orders_1.csv"]


def test_bad_header_goes_to_quarantine(lam):
    with open(os.path.join(ROOT, "sample_data", "bad_orders_missing_column.csv"), "rb") as f:
        lam.s3.put_object(Bucket=BUCKET, Key="incoming/bad.csv", Body=f.read())
    lam.lambda_handler(_event("incoming/bad.csv"), None)
    assert _keys(lam) == ["quarantine/2026-11-18/bad.csv"]
    meta = lam.s3.head_object(Bucket=BUCKET, Key="quarantine/2026-11-18/bad.csv")["Metadata"]
    assert "unit_price" in meta["reject_reason"]


def test_empty_and_wrong_extension(lam):
    lam.s3.put_object(Bucket=BUCKET, Key="incoming/empty.csv", Body=b"")
    lam.s3.put_object(Bucket=BUCKET, Key="incoming/notes.txt", Body=b"hello")
    lam.lambda_handler(_event("incoming/empty.csv"), None)
    lam.lambda_handler(_event("incoming/notes.txt"), None)
    assert _keys(lam) == ["quarantine/2026-11-18/empty.csv", "quarantine/2026-11-18/notes.txt"]
'''

FILES['docs/generate_diagrams.py'] = r'''"""Regenerates the README diagrams (SVG) into docs/images/.
Usage: python docs/generate_diagrams.py"""
import os
import sys
from xml.sax.saxutils import escape as esc

ROOT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
OUT = os.path.join(ROOT, "docs", "images")
os.makedirs(OUT, exist_ok=True)

FONT = "Arial, Helvetica, sans-serif"
C = {  # service colours (border, fill)
    "src": ("#6b7280", "#f3f4f6"),
    "s3": ("#3f8624", "#ecf7e8"),
    "lambda": ("#ed7100", "#fff3e6"),
    "glue": ("#8c4fff", "#f3edff"),
    "redshift": ("#2e73b8", "#e8f1fa"),
    "dq": ("#0f766e", "#e6f6f4"),
    "airflow": ("#017cee", "#e8f3fe"),
    "bad": ("#c0392b", "#fdecea"),
}


def head(w, h, title):
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
        f'font-family="{FONT}">',
        f"<title>{esc(title)}</title>",
        "<defs>"
        '<marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
        '<path d="M0,0 L10,5 L0,10 z" fill="#374151"/></marker>'
        '<marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
        '<path d="M0,0 L10,5 L0,10 z" fill="#017cee"/></marker>'
        "</defs>",
        f'<rect width="{w}" height="{h}" fill="#ffffff"/>',
    ]


def box(x, y, w, h, kind, title, lines=(), tag=None):
    stroke, fill = C[kind]
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{fill}" stroke="{stroke}" stroke-width="2"/>']
    if tag:
        out.append(f'<rect x="{x}" y="{y}" width="{w}" height="22" rx="10" fill="{stroke}"/>'
                   f'<rect x="{x}" y="{y+11}" width="{w}" height="11" fill="{stroke}"/>'
                   f'<text x="{x+w/2}" y="{y+15.5}" text-anchor="middle" font-size="11" font-weight="700" '
                   f'fill="#ffffff" letter-spacing="0.6">{esc(tag)}</text>')
        ty = y + 44
    else:
        ty = y + 28
    out.append(f'<text x="{x+w/2}" y="{ty}" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">{esc(title)}</text>')
    for i, ln in enumerate(lines):
        out.append(f'<text x="{x+w/2}" y="{ty+20+i*17}" text-anchor="middle" font-size="12" fill="#4b5563">{esc(ln)}</text>')
    return out


def arrow(points, label=None, lx=None, ly=None, color="#374151", dashed=False, anchor="middle"):
    d = "M" + " L".join(f"{x},{y}" for x, y in points)
    m = "ab" if color == "#017cee" else "a"
    dash = ' stroke-dasharray="6 5"' if dashed else ""
    out = [f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2"{dash} marker-end="url(#{m})"/>']
    if label:
        out.append(f'<text x="{lx}" y="{ly}" text-anchor="{anchor}" font-size="12" font-style="italic" fill="{color}">{esc(label)}</text>')
    return out


def save(name, parts):
    parts.append("</svg>")
    with open(os.path.join(OUT, name), "w") as f:
        f.write("\n".join(parts) + "\n")


# --------------------------------------------------------------- 1. architecture
W, H = 1080, 640
p = head(W, H, "AWS e-commerce pipeline architecture")
p.append(f'<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">End-to-end architecture</text>')
p.append(f'<text x="30" y="58" font-size="13" fill="#6b7280">Files flow left to right on the top row, then right to left on the bottom row.</text>')
bw, bh = 200, 96
X = [30, 310, 590, 850]
Y1, Y2 = 90, 360
p += box(X[0], Y1, bw, bh, "src", "Order files", ["CSV exports from the", "e-commerce store"], "SOURCE")
p += box(X[1], Y1, bw, bh, "s3", "S3 raw bucket", ["incoming/*.csv"], "AMAZON S3")
p += box(X[2], Y1, bw, bh, "lambda", "validate_and_trigger", ["checks type, size, header"], "AWS LAMBDA")
p += box(X[3], Y1, bw, bh, "s3", "S3 raw bucket", ["validated/<date>/*.csv"], "AMAZON S3")
p += box(X[2], 230, bw, 80, "bad", "Quarantine", ["quarantine/<date>/ + reason"])
p += box(X[3], Y2, bw, bh, "glue", "transform_orders", ["clean, type, dedupe", "(PySpark, Glue 4.0)"], "AWS GLUE")
p += box(X[2], Y2, bw, bh, "s3", "S3 curated bucket", ["orders/load_date=<ds>/", "*.parquet"], "AMAZON S3")
p += box(X[1], Y2, bw, bh, "redshift", "Star schema", ["staging → dim_* +", "fact_order_lines"], "AMAZON REDSHIFT")
p += box(X[0], Y2, bw, bh, "dq", "Quality checks", ["8 SQL checks;", "run fails on any error"], "DATA QUALITY")
cy1, cy2 = Y1 + bh / 2, Y2 + bh / 2
p += arrow([(X[0]+bw, cy1), (X[1]-4, cy1)], "upload", (X[0]+bw+X[1])/2, cy1-8)
p += arrow([(X[1]+bw, cy1), (X[2]-4, cy1)], "S3 event", (X[1]+bw+X[2])/2, cy1-8)
p += arrow([(X[2]+bw, cy1), (X[3]-4, cy1)], "valid", (X[2]+bw+X[3])/2, cy1-8)
p += arrow([(X[2]+bw/2, Y1+bh), (X[2]+bw/2, 226)], "invalid", X[2]+bw/2+8, 212, color="#c0392b", anchor="start")
p += arrow([(X[3]+bw/2, Y1+bh), (X[3]+bw/2, Y2-4)], "daily batch", X[3]+bw/2+8, 290, anchor="start")
p += arrow([(X[3], cy2), (X[2]+bw+4, cy2)], "Parquet", (X[2]+bw+X[3])/2, cy2-8)
p += arrow([(X[2], cy2), (X[1]+bw+4, cy2)], "COPY", (X[1]+bw+X[2])/2, cy2-8)
p += arrow([(X[1], cy2), (X[0]+bw+4, cy2)], "validate", (X[0]+bw+X[1])/2, cy2-8)
# Airflow bar
ay = 530
p.append(f'<rect x="30" y="{ay}" width="{X[3]+bw-30}" height="74" rx="10" fill="{C["airflow"][1]}" stroke="{C["airflow"][0]}" stroke-width="2"/>')
p.append(f'<text x="50" y="{ay+30}" font-size="15" font-weight="700" fill="#0b4f9c">Apache Airflow — ecommerce_pipeline DAG (daily)</text>')
p.append(f'<text x="50" y="{ay+52}" font-size="12" fill="#1f4f80">wait for validated/&lt;ds&gt;/ files  →  run Glue job  →  load Redshift (one transaction)  →  run quality checks</text>')
for xx in (X[3], X[1], X[0]):
    p += arrow([(xx+bw/2, ay), (xx+bw/2, Y2+bh+4)], color="#017cee", dashed=True)
p.append(f'<text x="{X[2]+bw/2}" y="{ay-12}" text-anchor="middle" font-size="12" font-style="italic" fill="#017cee">orchestrates</text>')
save("architecture.svg", p)

# --------------------------------------------------------------- 2. airflow DAG
W, H = 1080, 300
p = head(W, H, "Airflow DAG tasks")
p.append('<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">Airflow DAG: ecommerce_pipeline</text>')
p.append('<text x="30" y="58" font-size="13" fill="#6b7280">Runs daily. Each run handles the files that arrived on its logical date ({{ ds }}). Example below: ds = 2026-11-18.</text>')
tasks = [
    ("wait_for_orders_file", "S3KeySensor", ["validated/2026-11-18/*.csv", "skips the run if no files"]),
    ("glue_transform_orders", "GlueJobOperator", ["writes curated/orders/", "load_date=2026-11-18/"]),
    ("load_redshift", "SQLExecuteQueryOperator", ["COPY + dedupe + upsert", "8 statements, 1 transaction"]),
    ("data_quality_checks", "PythonOperator", ["8 checks from checks.sql", "fails the run on any error"]),
]
bw, gap = 225, 40
for i, (tid, op, lines) in enumerate(tasks):
    x = 30 + i * (bw + gap)
    p.append(f'<rect x="{x}" y="90" width="{bw}" height="150" rx="10" fill="#ffffff" stroke="#2e7d32" stroke-width="2.5"/>')
    p.append(f'<rect x="{x}" y="90" width="8" height="150" rx="4" fill="#2e7d32"/>')
    p.append(f'<text x="{x+22}" y="120" font-size="11" font-weight="700" fill="#6b7280">STEP {i+1}</text>')
    p.append(f'<text x="{x+22}" y="144" font-size="15" font-weight="700" fill="#111827">{esc(tid)}</text>')
    p.append(f'<text x="{x+22}" y="166" font-size="12" fill="#017cee">{esc(op)}</text>')
    for j, ln in enumerate(lines):
        p.append(f'<text x="{x+22}" y="{196+j*18}" font-size="12" fill="#4b5563">{esc(ln)}</text>')
    if i < 3:
        p += arrow([(x+bw, 165), (x+bw+gap-4, 165)])
p.append('<text x="30" y="278" font-size="12" fill="#6b7280">Retries: 2 (5 min apart)  ·  max_active_runs = 1  ·  catchup = False</text>')
save("airflow_dag.svg", p)

# --------------------------------------------------------------- 3. star schema
W, H = 1080, 540
p = head(W, H, "Redshift star schema")
p.append('<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">Redshift data model</text>')
p.append('<text x="30" y="58" font-size="13" fill="#6b7280">Parquet is copied into staging, deduplicated, then upserted into one fact table and two dimensions.</text>')
RH = 24


def table(x, y, w, name, cols, kind, note=None):
    stroke, fill = C[kind]
    h = 38 + RH * len(cols)
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="#ffffff" stroke="{stroke}" stroke-width="2"/>',
           f'<rect x="{x}" y="{y}" width="{w}" height="38" rx="8" fill="{stroke}"/>',
           f'<rect x="{x}" y="{y+30}" width="{w}" height="8" fill="{stroke}"/>',
           f'<text x="{x+w/2}" y="{y+25}" text-anchor="middle" font-size="14" font-weight="700" fill="#ffffff">{esc(name)}</text>']
    for i, (cname, typ, key) in enumerate(cols):
        ry = y + 38 + i * RH
        if i % 2:
            out.append(f'<rect x="{x+2}" y="{ry}" width="{w-4}" height="{RH}" fill="{fill}"/>')
        out.append(f'<text x="{x+14}" y="{ry+16}" font-size="12" font-weight="{700 if key else 400}" fill="#111827">{esc(cname)}</text>')
        out.append(f'<text x="{x+w-78}" y="{ry+16}" text-anchor="end" font-size="11" fill="#6b7280">{esc(typ)}</text>')
        if key:
            col = "#b45309" if key.startswith("PK") else ("#2e73b8" if key == "FK" else "#0f766e")
            out.append(f'<text x="{x+w-12}" y="{ry+16}" text-anchor="end" font-size="11" font-weight="700" fill="{col}">{esc(key)}</text>')
    if note:
        out.append(f'<text x="{x+w/2}" y="{y+h+20}" text-anchor="middle" font-size="11" fill="#6b7280">{esc(note)}</text>')
    return out, h


fact_cols = [
    ("order_id", "VARCHAR(64)", "PK"), ("product_id", "VARCHAR(64)", "PK, FK"),
    ("customer_id", "VARCHAR(64)", "FK"), ("order_ts", "TIMESTAMP", ""),
    ("order_date", "DATE", "SORTKEY"), ("quantity", "INTEGER", ""),
    ("unit_price", "DECIMAL(12,2)", ""), ("line_total", "DECIMAL(14,2)", ""),
    ("currency", "VARCHAR(3)", ""), ("order_status", "VARCHAR(32)", ""), ("load_ts", "TIMESTAMP", ""),
]
fx, fy, fw = 380, 170, 320
t, fh = table(fx, fy, fw, "analytics.fact_order_lines", fact_cols, "redshift", "DISTKEY customer_id  ·  SORTKEY order_date")
p += t
dw, dy = 260, 230
dc, dh = table(30, dy, dw, "analytics.dim_customer",
               [("customer_key", "BIGINT", "PK"), ("customer_id", "VARCHAR(64)", ""), ("first_seen_ts", "TIMESTAMP", "")],
               "dq", "DISTSTYLE ALL")
p += dc
dp, _ = table(790, dy, dw, "analytics.dim_product",
              [("product_key", "BIGINT", "PK"), ("product_id", "VARCHAR(64)", ""), ("first_seen_ts", "TIMESTAMP", "")],
              "dq", "DISTSTYLE ALL")
p += dp
cy_dim = dy + 38 + RH + RH / 2
cy_cust = fy + 38 + RH * 2 + RH / 2
cy_prod = fy + 38 + RH + RH / 2
p.append(f'<path d="M{30+dw},{cy_dim} L335,{cy_dim} L335,{cy_cust} L{fx},{cy_cust}" fill="none" stroke="#2e73b8" stroke-width="2"/>')
p.append(f'<path d="M790,{cy_dim} L745,{cy_dim} L745,{cy_prod} L{fx+fw},{cy_prod}" fill="none" stroke="#2e73b8" stroke-width="2"/>')
for lx, ly in ((335, (cy_dim + cy_cust) / 2), (745, (cy_dim + cy_prod) / 2)):
    p.append(f'<rect x="{lx-28}" y="{ly-10}" width="56" height="18" rx="4" fill="#ffffff" stroke="#2e73b8"/>')
    p.append(f'<text x="{lx}" y="{ly+3.5}" text-anchor="middle" font-size="11" fill="#2e73b8">1 : many</text>')
# staging
p.append(f'<rect x="{fx}" y="80" width="{fw}" height="56" rx="10" fill="{C["s3"][1]}" stroke="{C["s3"][0]}" stroke-width="2"/>')
p.append(f'<text x="{fx+fw/2}" y="103" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">staging.orders_stg</text>')
p.append(f'<text x="{fx+fw/2}" y="123" text-anchor="middle" font-size="12" fill="#4b5563">11 columns, same order as the Glue output</text>')
p += arrow([(fx+fw/2, 136), (fx+fw/2, fy-4)])
p.append(f'<text x="{fx+fw/2+10}" y="{(136+fy)/2+4}" font-size="12" font-style="italic" fill="#374151">dedupe + upsert</text>')
p.append(f'<text x="30" y="{H-20}" font-size="12" fill="#6b7280">PK = primary key  ·  FK = links to a dimension  ·  SORTKEY speeds up date filters  ·  DISTSTYLE ALL copies small tables to every node</text>')
save("star_schema.svg", p)

# --------------------------------------------------------------- 4. data cleaning
rows = [
    ("O1001", "P100", "2", "19.99", "kept as is", True),
    ("O1001", "P200", "1", "5.50", "kept as is", True),
    ("O1002", "P100", "1", "19.99", "older version of the same order line (10:02)", False),
    ("O1002", "P100", "3", "19.99", "latest version (11:45) wins the dedupe", True),
    ("O1003", "P300", "0", "49.00", "quantity is 0", False),
    ("(blank)", "P100", "1", "19.99", "missing order_id", False),
    ("O1004", "P100", "1", "19.99", "order_ts is \"not-a-date\"", False),
    ("O1005", "P400", "1", "-3.00", "negative unit_price", False),
    ("O1006", "P500", "4", "12.25", "\" C006 \" trimmed, \"eur\" → EUR", True),
]
W = 1080
RH = 34
H = 150 + RH * len(rows) + 60
p = head(W, H, "Glue transform on the sample data")
p.append('<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">What the Glue job does to the sample file</text>')
p.append('<text x="30" y="58" font-size="13" fill="#6b7280">sample_data/orders_sample.csv  →  output/transformed_orders_sample.csv</text>')
cols = [("order_id", 30), ("product_id", 140), ("quantity", 260), ("unit_price", 360), ("What happened", 480), ("Result", 920)]
ty = 90
p.append(f'<rect x="20" y="{ty}" width="{W-40}" height="36" rx="6" fill="#1f2937"/>')
for name, x in cols:
    p.append(f'<text x="{x+10}" y="{ty+23}" font-size="13" font-weight="700" fill="#ffffff">{esc(name)}</text>')
for i, (oid, pid, q, price, what, kept) in enumerate(rows):
    y = ty + 36 + i * RH
    fill = "#ecf7e8" if kept else "#fdecea"
    p.append(f'<rect x="20" y="{y}" width="{W-40}" height="{RH}" fill="{fill}"/>')
    p.append(f'<line x1="20" y1="{y+RH}" x2="{W-20}" y2="{y+RH}" stroke="#ffffff" stroke-width="2"/>')
    vals = [oid, pid, q, price, what]
    for (name, x), v in zip(cols[:5], vals):
        p.append(f'<text x="{x+10}" y="{y+22}" font-size="13" fill="#111827">{esc(v)}</text>')
    label, col = ("kept", "#2e7d32") if kept else ("dropped", "#c0392b")
    ix, iy = cols[5][1] + 18, y + RH / 2
    p.append(f'<circle cx="{ix}" cy="{iy}" r="9" fill="{col}"/>')
    if kept:
        p.append(f'<path d="M{ix-4.5},{iy} L{ix-1},{iy+3.5} L{ix+5},{iy-3.5}" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>')
    else:
        p.append(f'<path d="M{ix-3.5},{iy-3.5} L{ix+3.5},{iy+3.5} M{ix+3.5},{iy-3.5} L{ix-3.5},{iy+3.5}" stroke="#fff" stroke-width="2.2" stroke-linecap="round"/>')
    p.append(f'<text x="{ix+16}" y="{y+22}" font-size="13" font-weight="700" fill="{col}">{label}</text>')
sy = ty + 36 + RH * len(rows) + 34
p.append(f'<text x="30" y="{sy}" font-size="15" font-weight="700" fill="#111827">9 rows in  →  4 clean rows out</text>')
p.append(f'<text x="290" y="{sy}" font-size="13" fill="#4b5563">The job also adds line_total (quantity × unit_price) and load_ts, and lower-cases order_status.</text>')
save("data_cleaning.svg", p)

print("wrote", sorted(os.listdir(OUT)))
'''

FILES['docs/images/airflow_dag.svg'] = r'''<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="300" viewBox="0 0 1080 300" font-family="Arial, Helvetica, sans-serif">
<title>Airflow DAG tasks</title>
<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#374151"/></marker><marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#017cee"/></marker></defs>
<rect width="1080" height="300" fill="#ffffff"/>
<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">Airflow DAG: ecommerce_pipeline</text>
<text x="30" y="58" font-size="13" fill="#6b7280">Runs daily. Each run handles the files that arrived on its logical date ({{ ds }}). Example below: ds = 2026-11-18.</text>
<rect x="30" y="90" width="225" height="150" rx="10" fill="#ffffff" stroke="#2e7d32" stroke-width="2.5"/>
<rect x="30" y="90" width="8" height="150" rx="4" fill="#2e7d32"/>
<text x="52" y="120" font-size="11" font-weight="700" fill="#6b7280">STEP 1</text>
<text x="52" y="144" font-size="15" font-weight="700" fill="#111827">wait_for_orders_file</text>
<text x="52" y="166" font-size="12" fill="#017cee">S3KeySensor</text>
<text x="52" y="196" font-size="12" fill="#4b5563">validated/2026-11-18/*.csv</text>
<text x="52" y="214" font-size="12" fill="#4b5563">skips the run if no files</text>
<path d="M255,165 L291,165" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<rect x="295" y="90" width="225" height="150" rx="10" fill="#ffffff" stroke="#2e7d32" stroke-width="2.5"/>
<rect x="295" y="90" width="8" height="150" rx="4" fill="#2e7d32"/>
<text x="317" y="120" font-size="11" font-weight="700" fill="#6b7280">STEP 2</text>
<text x="317" y="144" font-size="15" font-weight="700" fill="#111827">glue_transform_orders</text>
<text x="317" y="166" font-size="12" fill="#017cee">GlueJobOperator</text>
<text x="317" y="196" font-size="12" fill="#4b5563">writes curated/orders/</text>
<text x="317" y="214" font-size="12" fill="#4b5563">load_date=2026-11-18/</text>
<path d="M520,165 L556,165" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<rect x="560" y="90" width="225" height="150" rx="10" fill="#ffffff" stroke="#2e7d32" stroke-width="2.5"/>
<rect x="560" y="90" width="8" height="150" rx="4" fill="#2e7d32"/>
<text x="582" y="120" font-size="11" font-weight="700" fill="#6b7280">STEP 3</text>
<text x="582" y="144" font-size="15" font-weight="700" fill="#111827">load_redshift</text>
<text x="582" y="166" font-size="12" fill="#017cee">SQLExecuteQueryOperator</text>
<text x="582" y="196" font-size="12" fill="#4b5563">COPY + dedupe + upsert</text>
<text x="582" y="214" font-size="12" fill="#4b5563">8 statements, 1 transaction</text>
<path d="M785,165 L821,165" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<rect x="825" y="90" width="225" height="150" rx="10" fill="#ffffff" stroke="#2e7d32" stroke-width="2.5"/>
<rect x="825" y="90" width="8" height="150" rx="4" fill="#2e7d32"/>
<text x="847" y="120" font-size="11" font-weight="700" fill="#6b7280">STEP 4</text>
<text x="847" y="144" font-size="15" font-weight="700" fill="#111827">data_quality_checks</text>
<text x="847" y="166" font-size="12" fill="#017cee">PythonOperator</text>
<text x="847" y="196" font-size="12" fill="#4b5563">8 checks from checks.sql</text>
<text x="847" y="214" font-size="12" fill="#4b5563">fails the run on any error</text>
<text x="30" y="278" font-size="12" fill="#6b7280">Retries: 2 (5 min apart)  ·  max_active_runs = 1  ·  catchup = False</text>
</svg>
'''

FILES['docs/images/architecture.svg'] = r'''<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="640" viewBox="0 0 1080 640" font-family="Arial, Helvetica, sans-serif">
<title>AWS e-commerce pipeline architecture</title>
<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#374151"/></marker><marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#017cee"/></marker></defs>
<rect width="1080" height="640" fill="#ffffff"/>
<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">End-to-end architecture</text>
<text x="30" y="58" font-size="13" fill="#6b7280">Files flow left to right on the top row, then right to left on the bottom row.</text>
<rect x="30" y="90" width="200" height="96" rx="10" fill="#f3f4f6" stroke="#6b7280" stroke-width="2"/>
<rect x="30" y="90" width="200" height="22" rx="10" fill="#6b7280"/><rect x="30" y="101" width="200" height="11" fill="#6b7280"/><text x="130.0" y="105.5" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff" letter-spacing="0.6">SOURCE</text>
<text x="130.0" y="134" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">Order files</text>
<text x="130.0" y="154" text-anchor="middle" font-size="12" fill="#4b5563">CSV exports from the</text>
<text x="130.0" y="171" text-anchor="middle" font-size="12" fill="#4b5563">e-commerce store</text>
<rect x="310" y="90" width="200" height="96" rx="10" fill="#ecf7e8" stroke="#3f8624" stroke-width="2"/>
<rect x="310" y="90" width="200" height="22" rx="10" fill="#3f8624"/><rect x="310" y="101" width="200" height="11" fill="#3f8624"/><text x="410.0" y="105.5" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff" letter-spacing="0.6">AMAZON S3</text>
<text x="410.0" y="134" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">S3 raw bucket</text>
<text x="410.0" y="154" text-anchor="middle" font-size="12" fill="#4b5563">incoming/*.csv</text>
<rect x="590" y="90" width="200" height="96" rx="10" fill="#fff3e6" stroke="#ed7100" stroke-width="2"/>
<rect x="590" y="90" width="200" height="22" rx="10" fill="#ed7100"/><rect x="590" y="101" width="200" height="11" fill="#ed7100"/><text x="690.0" y="105.5" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff" letter-spacing="0.6">AWS LAMBDA</text>
<text x="690.0" y="134" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">validate_and_trigger</text>
<text x="690.0" y="154" text-anchor="middle" font-size="12" fill="#4b5563">checks type, size, header</text>
<rect x="850" y="90" width="200" height="96" rx="10" fill="#ecf7e8" stroke="#3f8624" stroke-width="2"/>
<rect x="850" y="90" width="200" height="22" rx="10" fill="#3f8624"/><rect x="850" y="101" width="200" height="11" fill="#3f8624"/><text x="950.0" y="105.5" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff" letter-spacing="0.6">AMAZON S3</text>
<text x="950.0" y="134" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">S3 raw bucket</text>
<text x="950.0" y="154" text-anchor="middle" font-size="12" fill="#4b5563">validated/&lt;date&gt;/*.csv</text>
<rect x="590" y="230" width="200" height="80" rx="10" fill="#fdecea" stroke="#c0392b" stroke-width="2"/>
<text x="690.0" y="258" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">Quarantine</text>
<text x="690.0" y="278" text-anchor="middle" font-size="12" fill="#4b5563">quarantine/&lt;date&gt;/ + reason</text>
<rect x="850" y="360" width="200" height="96" rx="10" fill="#f3edff" stroke="#8c4fff" stroke-width="2"/>
<rect x="850" y="360" width="200" height="22" rx="10" fill="#8c4fff"/><rect x="850" y="371" width="200" height="11" fill="#8c4fff"/><text x="950.0" y="375.5" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff" letter-spacing="0.6">AWS GLUE</text>
<text x="950.0" y="404" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">transform_orders</text>
<text x="950.0" y="424" text-anchor="middle" font-size="12" fill="#4b5563">clean, type, dedupe</text>
<text x="950.0" y="441" text-anchor="middle" font-size="12" fill="#4b5563">(PySpark, Glue 4.0)</text>
<rect x="590" y="360" width="200" height="96" rx="10" fill="#ecf7e8" stroke="#3f8624" stroke-width="2"/>
<rect x="590" y="360" width="200" height="22" rx="10" fill="#3f8624"/><rect x="590" y="371" width="200" height="11" fill="#3f8624"/><text x="690.0" y="375.5" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff" letter-spacing="0.6">AMAZON S3</text>
<text x="690.0" y="404" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">S3 curated bucket</text>
<text x="690.0" y="424" text-anchor="middle" font-size="12" fill="#4b5563">orders/load_date=&lt;ds&gt;/</text>
<text x="690.0" y="441" text-anchor="middle" font-size="12" fill="#4b5563">*.parquet</text>
<rect x="310" y="360" width="200" height="96" rx="10" fill="#e8f1fa" stroke="#2e73b8" stroke-width="2"/>
<rect x="310" y="360" width="200" height="22" rx="10" fill="#2e73b8"/><rect x="310" y="371" width="200" height="11" fill="#2e73b8"/><text x="410.0" y="375.5" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff" letter-spacing="0.6">AMAZON REDSHIFT</text>
<text x="410.0" y="404" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">Star schema</text>
<text x="410.0" y="424" text-anchor="middle" font-size="12" fill="#4b5563">staging → dim_* +</text>
<text x="410.0" y="441" text-anchor="middle" font-size="12" fill="#4b5563">fact_order_lines</text>
<rect x="30" y="360" width="200" height="96" rx="10" fill="#e6f6f4" stroke="#0f766e" stroke-width="2"/>
<rect x="30" y="360" width="200" height="22" rx="10" fill="#0f766e"/><rect x="30" y="371" width="200" height="11" fill="#0f766e"/><text x="130.0" y="375.5" text-anchor="middle" font-size="11" font-weight="700" fill="#ffffff" letter-spacing="0.6">DATA QUALITY</text>
<text x="130.0" y="404" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">Quality checks</text>
<text x="130.0" y="424" text-anchor="middle" font-size="12" fill="#4b5563">8 SQL checks;</text>
<text x="130.0" y="441" text-anchor="middle" font-size="12" fill="#4b5563">run fails on any error</text>
<path d="M230,138.0 L306,138.0" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<text x="270.0" y="130.0" text-anchor="middle" font-size="12" font-style="italic" fill="#374151">upload</text>
<path d="M510,138.0 L586,138.0" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<text x="550.0" y="130.0" text-anchor="middle" font-size="12" font-style="italic" fill="#374151">S3 event</text>
<path d="M790,138.0 L846,138.0" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<text x="820.0" y="130.0" text-anchor="middle" font-size="12" font-style="italic" fill="#374151">valid</text>
<path d="M690.0,186 L690.0,226" fill="none" stroke="#c0392b" stroke-width="2" marker-end="url(#a)"/>
<text x="698.0" y="212" text-anchor="start" font-size="12" font-style="italic" fill="#c0392b">invalid</text>
<path d="M950.0,186 L950.0,356" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<text x="958.0" y="290" text-anchor="start" font-size="12" font-style="italic" fill="#374151">daily batch</text>
<path d="M850,408.0 L794,408.0" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<text x="820.0" y="400.0" text-anchor="middle" font-size="12" font-style="italic" fill="#374151">Parquet</text>
<path d="M590,408.0 L514,408.0" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<text x="550.0" y="400.0" text-anchor="middle" font-size="12" font-style="italic" fill="#374151">COPY</text>
<path d="M310,408.0 L234,408.0" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<text x="270.0" y="400.0" text-anchor="middle" font-size="12" font-style="italic" fill="#374151">validate</text>
<rect x="30" y="530" width="1020" height="74" rx="10" fill="#e8f3fe" stroke="#017cee" stroke-width="2"/>
<text x="50" y="560" font-size="15" font-weight="700" fill="#0b4f9c">Apache Airflow — ecommerce_pipeline DAG (daily)</text>
<text x="50" y="582" font-size="12" fill="#1f4f80">wait for validated/&lt;ds&gt;/ files  →  run Glue job  →  load Redshift (one transaction)  →  run quality checks</text>
<path d="M950.0,530 L950.0,460" fill="none" stroke="#017cee" stroke-width="2" stroke-dasharray="6 5" marker-end="url(#ab)"/>
<path d="M410.0,530 L410.0,460" fill="none" stroke="#017cee" stroke-width="2" stroke-dasharray="6 5" marker-end="url(#ab)"/>
<path d="M130.0,530 L130.0,460" fill="none" stroke="#017cee" stroke-width="2" stroke-dasharray="6 5" marker-end="url(#ab)"/>
<text x="690.0" y="518" text-anchor="middle" font-size="12" font-style="italic" fill="#017cee">orchestrates</text>
</svg>
'''

FILES['docs/images/data_cleaning.svg'] = r'''<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="516" viewBox="0 0 1080 516" font-family="Arial, Helvetica, sans-serif">
<title>Glue transform on the sample data</title>
<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#374151"/></marker><marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#017cee"/></marker></defs>
<rect width="1080" height="516" fill="#ffffff"/>
<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">What the Glue job does to the sample file</text>
<text x="30" y="58" font-size="13" fill="#6b7280">sample_data/orders_sample.csv  →  output/transformed_orders_sample.csv</text>
<rect x="20" y="90" width="1040" height="36" rx="6" fill="#1f2937"/>
<text x="40" y="113" font-size="13" font-weight="700" fill="#ffffff">order_id</text>
<text x="150" y="113" font-size="13" font-weight="700" fill="#ffffff">product_id</text>
<text x="270" y="113" font-size="13" font-weight="700" fill="#ffffff">quantity</text>
<text x="370" y="113" font-size="13" font-weight="700" fill="#ffffff">unit_price</text>
<text x="490" y="113" font-size="13" font-weight="700" fill="#ffffff">What happened</text>
<text x="930" y="113" font-size="13" font-weight="700" fill="#ffffff">Result</text>
<rect x="20" y="126" width="1040" height="34" fill="#ecf7e8"/>
<line x1="20" y1="160" x2="1060" y2="160" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="148" font-size="13" fill="#111827">O1001</text>
<text x="150" y="148" font-size="13" fill="#111827">P100</text>
<text x="270" y="148" font-size="13" fill="#111827">2</text>
<text x="370" y="148" font-size="13" fill="#111827">19.99</text>
<text x="490" y="148" font-size="13" fill="#111827">kept as is</text>
<circle cx="938" cy="143.0" r="9" fill="#2e7d32"/>
<path d="M933.5,143.0 L937,146.5 L943,139.5" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
<text x="954" y="148" font-size="13" font-weight="700" fill="#2e7d32">kept</text>
<rect x="20" y="160" width="1040" height="34" fill="#ecf7e8"/>
<line x1="20" y1="194" x2="1060" y2="194" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="182" font-size="13" fill="#111827">O1001</text>
<text x="150" y="182" font-size="13" fill="#111827">P200</text>
<text x="270" y="182" font-size="13" fill="#111827">1</text>
<text x="370" y="182" font-size="13" fill="#111827">5.50</text>
<text x="490" y="182" font-size="13" fill="#111827">kept as is</text>
<circle cx="938" cy="177.0" r="9" fill="#2e7d32"/>
<path d="M933.5,177.0 L937,180.5 L943,173.5" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
<text x="954" y="182" font-size="13" font-weight="700" fill="#2e7d32">kept</text>
<rect x="20" y="194" width="1040" height="34" fill="#fdecea"/>
<line x1="20" y1="228" x2="1060" y2="228" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="216" font-size="13" fill="#111827">O1002</text>
<text x="150" y="216" font-size="13" fill="#111827">P100</text>
<text x="270" y="216" font-size="13" fill="#111827">1</text>
<text x="370" y="216" font-size="13" fill="#111827">19.99</text>
<text x="490" y="216" font-size="13" fill="#111827">older version of the same order line (10:02)</text>
<circle cx="938" cy="211.0" r="9" fill="#c0392b"/>
<path d="M934.5,207.5 L941.5,214.5 M941.5,207.5 L934.5,214.5" stroke="#fff" stroke-width="2.2" stroke-linecap="round"/>
<text x="954" y="216" font-size="13" font-weight="700" fill="#c0392b">dropped</text>
<rect x="20" y="228" width="1040" height="34" fill="#ecf7e8"/>
<line x1="20" y1="262" x2="1060" y2="262" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="250" font-size="13" fill="#111827">O1002</text>
<text x="150" y="250" font-size="13" fill="#111827">P100</text>
<text x="270" y="250" font-size="13" fill="#111827">3</text>
<text x="370" y="250" font-size="13" fill="#111827">19.99</text>
<text x="490" y="250" font-size="13" fill="#111827">latest version (11:45) wins the dedupe</text>
<circle cx="938" cy="245.0" r="9" fill="#2e7d32"/>
<path d="M933.5,245.0 L937,248.5 L943,241.5" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
<text x="954" y="250" font-size="13" font-weight="700" fill="#2e7d32">kept</text>
<rect x="20" y="262" width="1040" height="34" fill="#fdecea"/>
<line x1="20" y1="296" x2="1060" y2="296" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="284" font-size="13" fill="#111827">O1003</text>
<text x="150" y="284" font-size="13" fill="#111827">P300</text>
<text x="270" y="284" font-size="13" fill="#111827">0</text>
<text x="370" y="284" font-size="13" fill="#111827">49.00</text>
<text x="490" y="284" font-size="13" fill="#111827">quantity is 0</text>
<circle cx="938" cy="279.0" r="9" fill="#c0392b"/>
<path d="M934.5,275.5 L941.5,282.5 M941.5,275.5 L934.5,282.5" stroke="#fff" stroke-width="2.2" stroke-linecap="round"/>
<text x="954" y="284" font-size="13" font-weight="700" fill="#c0392b">dropped</text>
<rect x="20" y="296" width="1040" height="34" fill="#fdecea"/>
<line x1="20" y1="330" x2="1060" y2="330" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="318" font-size="13" fill="#111827">(blank)</text>
<text x="150" y="318" font-size="13" fill="#111827">P100</text>
<text x="270" y="318" font-size="13" fill="#111827">1</text>
<text x="370" y="318" font-size="13" fill="#111827">19.99</text>
<text x="490" y="318" font-size="13" fill="#111827">missing order_id</text>
<circle cx="938" cy="313.0" r="9" fill="#c0392b"/>
<path d="M934.5,309.5 L941.5,316.5 M941.5,309.5 L934.5,316.5" stroke="#fff" stroke-width="2.2" stroke-linecap="round"/>
<text x="954" y="318" font-size="13" font-weight="700" fill="#c0392b">dropped</text>
<rect x="20" y="330" width="1040" height="34" fill="#fdecea"/>
<line x1="20" y1="364" x2="1060" y2="364" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="352" font-size="13" fill="#111827">O1004</text>
<text x="150" y="352" font-size="13" fill="#111827">P100</text>
<text x="270" y="352" font-size="13" fill="#111827">1</text>
<text x="370" y="352" font-size="13" fill="#111827">19.99</text>
<text x="490" y="352" font-size="13" fill="#111827">order_ts is "not-a-date"</text>
<circle cx="938" cy="347.0" r="9" fill="#c0392b"/>
<path d="M934.5,343.5 L941.5,350.5 M941.5,343.5 L934.5,350.5" stroke="#fff" stroke-width="2.2" stroke-linecap="round"/>
<text x="954" y="352" font-size="13" font-weight="700" fill="#c0392b">dropped</text>
<rect x="20" y="364" width="1040" height="34" fill="#fdecea"/>
<line x1="20" y1="398" x2="1060" y2="398" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="386" font-size="13" fill="#111827">O1005</text>
<text x="150" y="386" font-size="13" fill="#111827">P400</text>
<text x="270" y="386" font-size="13" fill="#111827">1</text>
<text x="370" y="386" font-size="13" fill="#111827">-3.00</text>
<text x="490" y="386" font-size="13" fill="#111827">negative unit_price</text>
<circle cx="938" cy="381.0" r="9" fill="#c0392b"/>
<path d="M934.5,377.5 L941.5,384.5 M941.5,377.5 L934.5,384.5" stroke="#fff" stroke-width="2.2" stroke-linecap="round"/>
<text x="954" y="386" font-size="13" font-weight="700" fill="#c0392b">dropped</text>
<rect x="20" y="398" width="1040" height="34" fill="#ecf7e8"/>
<line x1="20" y1="432" x2="1060" y2="432" stroke="#ffffff" stroke-width="2"/>
<text x="40" y="420" font-size="13" fill="#111827">O1006</text>
<text x="150" y="420" font-size="13" fill="#111827">P500</text>
<text x="270" y="420" font-size="13" fill="#111827">4</text>
<text x="370" y="420" font-size="13" fill="#111827">12.25</text>
<text x="490" y="420" font-size="13" fill="#111827">" C006 " trimmed, "eur" → EUR</text>
<circle cx="938" cy="415.0" r="9" fill="#2e7d32"/>
<path d="M933.5,415.0 L937,418.5 L943,411.5" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
<text x="954" y="420" font-size="13" font-weight="700" fill="#2e7d32">kept</text>
<text x="30" y="466" font-size="15" font-weight="700" fill="#111827">9 rows in  →  4 clean rows out</text>
<text x="290" y="466" font-size="13" fill="#4b5563">The job also adds line_total (quantity × unit_price) and load_ts, and lower-cases order_status.</text>
</svg>
'''

FILES['docs/images/star_schema.svg'] = r'''<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="540" viewBox="0 0 1080 540" font-family="Arial, Helvetica, sans-serif">
<title>Redshift star schema</title>
<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#374151"/></marker><marker id="ab" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#017cee"/></marker></defs>
<rect width="1080" height="540" fill="#ffffff"/>
<text x="30" y="36" font-size="20" font-weight="700" fill="#111827">Redshift data model</text>
<text x="30" y="58" font-size="13" fill="#6b7280">Parquet is copied into staging, deduplicated, then upserted into one fact table and two dimensions.</text>
<rect x="380" y="170" width="320" height="302" rx="8" fill="#ffffff" stroke="#2e73b8" stroke-width="2"/>
<rect x="380" y="170" width="320" height="38" rx="8" fill="#2e73b8"/>
<rect x="380" y="200" width="320" height="8" fill="#2e73b8"/>
<text x="540.0" y="195" text-anchor="middle" font-size="14" font-weight="700" fill="#ffffff">analytics.fact_order_lines</text>
<text x="394" y="224" font-size="12" font-weight="700" fill="#111827">order_id</text>
<text x="622" y="224" text-anchor="end" font-size="11" fill="#6b7280">VARCHAR(64)</text>
<text x="688" y="224" text-anchor="end" font-size="11" font-weight="700" fill="#b45309">PK</text>
<rect x="382" y="232" width="316" height="24" fill="#e8f1fa"/>
<text x="394" y="248" font-size="12" font-weight="700" fill="#111827">product_id</text>
<text x="622" y="248" text-anchor="end" font-size="11" fill="#6b7280">VARCHAR(64)</text>
<text x="688" y="248" text-anchor="end" font-size="11" font-weight="700" fill="#b45309">PK, FK</text>
<text x="394" y="272" font-size="12" font-weight="700" fill="#111827">customer_id</text>
<text x="622" y="272" text-anchor="end" font-size="11" fill="#6b7280">VARCHAR(64)</text>
<text x="688" y="272" text-anchor="end" font-size="11" font-weight="700" fill="#2e73b8">FK</text>
<rect x="382" y="280" width="316" height="24" fill="#e8f1fa"/>
<text x="394" y="296" font-size="12" font-weight="400" fill="#111827">order_ts</text>
<text x="622" y="296" text-anchor="end" font-size="11" fill="#6b7280">TIMESTAMP</text>
<text x="394" y="320" font-size="12" font-weight="700" fill="#111827">order_date</text>
<text x="622" y="320" text-anchor="end" font-size="11" fill="#6b7280">DATE</text>
<text x="688" y="320" text-anchor="end" font-size="11" font-weight="700" fill="#0f766e">SORTKEY</text>
<rect x="382" y="328" width="316" height="24" fill="#e8f1fa"/>
<text x="394" y="344" font-size="12" font-weight="400" fill="#111827">quantity</text>
<text x="622" y="344" text-anchor="end" font-size="11" fill="#6b7280">INTEGER</text>
<text x="394" y="368" font-size="12" font-weight="400" fill="#111827">unit_price</text>
<text x="622" y="368" text-anchor="end" font-size="11" fill="#6b7280">DECIMAL(12,2)</text>
<rect x="382" y="376" width="316" height="24" fill="#e8f1fa"/>
<text x="394" y="392" font-size="12" font-weight="400" fill="#111827">line_total</text>
<text x="622" y="392" text-anchor="end" font-size="11" fill="#6b7280">DECIMAL(14,2)</text>
<text x="394" y="416" font-size="12" font-weight="400" fill="#111827">currency</text>
<text x="622" y="416" text-anchor="end" font-size="11" fill="#6b7280">VARCHAR(3)</text>
<rect x="382" y="424" width="316" height="24" fill="#e8f1fa"/>
<text x="394" y="440" font-size="12" font-weight="400" fill="#111827">order_status</text>
<text x="622" y="440" text-anchor="end" font-size="11" fill="#6b7280">VARCHAR(32)</text>
<text x="394" y="464" font-size="12" font-weight="400" fill="#111827">load_ts</text>
<text x="622" y="464" text-anchor="end" font-size="11" fill="#6b7280">TIMESTAMP</text>
<text x="540.0" y="492" text-anchor="middle" font-size="11" fill="#6b7280">DISTKEY customer_id  ·  SORTKEY order_date</text>
<rect x="30" y="230" width="260" height="110" rx="8" fill="#ffffff" stroke="#0f766e" stroke-width="2"/>
<rect x="30" y="230" width="260" height="38" rx="8" fill="#0f766e"/>
<rect x="30" y="260" width="260" height="8" fill="#0f766e"/>
<text x="160.0" y="255" text-anchor="middle" font-size="14" font-weight="700" fill="#ffffff">analytics.dim_customer</text>
<text x="44" y="284" font-size="12" font-weight="700" fill="#111827">customer_key</text>
<text x="212" y="284" text-anchor="end" font-size="11" fill="#6b7280">BIGINT</text>
<text x="278" y="284" text-anchor="end" font-size="11" font-weight="700" fill="#b45309">PK</text>
<rect x="32" y="292" width="256" height="24" fill="#e6f6f4"/>
<text x="44" y="308" font-size="12" font-weight="400" fill="#111827">customer_id</text>
<text x="212" y="308" text-anchor="end" font-size="11" fill="#6b7280">VARCHAR(64)</text>
<text x="44" y="332" font-size="12" font-weight="400" fill="#111827">first_seen_ts</text>
<text x="212" y="332" text-anchor="end" font-size="11" fill="#6b7280">TIMESTAMP</text>
<text x="160.0" y="360" text-anchor="middle" font-size="11" fill="#6b7280">DISTSTYLE ALL</text>
<rect x="790" y="230" width="260" height="110" rx="8" fill="#ffffff" stroke="#0f766e" stroke-width="2"/>
<rect x="790" y="230" width="260" height="38" rx="8" fill="#0f766e"/>
<rect x="790" y="260" width="260" height="8" fill="#0f766e"/>
<text x="920.0" y="255" text-anchor="middle" font-size="14" font-weight="700" fill="#ffffff">analytics.dim_product</text>
<text x="804" y="284" font-size="12" font-weight="700" fill="#111827">product_key</text>
<text x="972" y="284" text-anchor="end" font-size="11" fill="#6b7280">BIGINT</text>
<text x="1038" y="284" text-anchor="end" font-size="11" font-weight="700" fill="#b45309">PK</text>
<rect x="792" y="292" width="256" height="24" fill="#e6f6f4"/>
<text x="804" y="308" font-size="12" font-weight="400" fill="#111827">product_id</text>
<text x="972" y="308" text-anchor="end" font-size="11" fill="#6b7280">VARCHAR(64)</text>
<text x="804" y="332" font-size="12" font-weight="400" fill="#111827">first_seen_ts</text>
<text x="972" y="332" text-anchor="end" font-size="11" fill="#6b7280">TIMESTAMP</text>
<text x="920.0" y="360" text-anchor="middle" font-size="11" fill="#6b7280">DISTSTYLE ALL</text>
<path d="M290,304.0 L335,304.0 L335,268.0 L380,268.0" fill="none" stroke="#2e73b8" stroke-width="2"/>
<path d="M790,304.0 L745,304.0 L745,244.0 L700,244.0" fill="none" stroke="#2e73b8" stroke-width="2"/>
<rect x="307" y="276.0" width="56" height="18" rx="4" fill="#ffffff" stroke="#2e73b8"/>
<text x="335" y="289.5" text-anchor="middle" font-size="11" fill="#2e73b8">1 : many</text>
<rect x="717" y="264.0" width="56" height="18" rx="4" fill="#ffffff" stroke="#2e73b8"/>
<text x="745" y="277.5" text-anchor="middle" font-size="11" fill="#2e73b8">1 : many</text>
<rect x="380" y="80" width="320" height="56" rx="10" fill="#ecf7e8" stroke="#3f8624" stroke-width="2"/>
<text x="540.0" y="103" text-anchor="middle" font-size="15" font-weight="700" fill="#111827">staging.orders_stg</text>
<text x="540.0" y="123" text-anchor="middle" font-size="12" fill="#4b5563">11 columns, same order as the Glue output</text>
<path d="M540.0,136 L540.0,166" fill="none" stroke="#374151" stroke-width="2" marker-end="url(#a)"/>
<text x="550.0" y="157.0" font-size="12" font-style="italic" fill="#374151">dedupe + upsert</text>
<text x="30" y="520" font-size="12" fill="#6b7280">PK = primary key  ·  FK = links to a dimension  ·  SORTKEY speeds up date filters  ·  DISTSTYLE ALL copies small tables to every node</text>
</svg>
'''

FILES['output/README.md'] = r'''# Sample output

Generated locally from `sample_data/` (no AWS account needed).

| File | What it shows |
|------|---------------|
| `test_results.txt` | pytest run: Lambda (moto S3), Glue transform (local Spark), SQL checks |
| `test_results_dag.txt` | pytest run of the Airflow DAG under Airflow 2.10 |
| `lambda_routing_log.txt` | Lambda routing good files to `validated/` and bad files to `quarantine/` |
| `transformed_orders_sample.csv` | Glue `transform()` output for `orders_sample.csv` (9 rows in → 4 clean rows out) |
| `transform_summary.txt` | Row counts and why rows were dropped |
| `rendered_dag_paths.txt` | S3 paths the DAG resolves for logical date 2026-11-18 |
'''

FILES['output/lambda_routing_log.txt'] = r'''{"status": "validated", "key": "incoming/orders_sample.csv", "dest": "validated/2026-11-18/orders_sample.csv"}
{"status": "quarantined", "key": "incoming/bad_orders_missing_column.csv", "dest": "quarantine/2026-11-18/bad_orders_missing_column.csv", "reason": "missing columns: ['unit_price']"}
{"status": "quarantined", "key": "incoming/empty.csv", "dest": "quarantine/2026-11-18/empty.csv", "reason": "empty file"}

Bucket after Lambda:
  s3://ecom-raw/quarantine/2026-11-18/bad_orders_missing_column.csv
  s3://ecom-raw/quarantine/2026-11-18/empty.csv
  s3://ecom-raw/validated/2026-11-18/orders_sample.csv
'''

FILES['output/rendered_dag_paths.txt'] = r'''Logical date (ds): 2026-11-18

wait_for_orders_file  bucket_key : validated/2026-11-18/*.csv
glue_transform_orders --source_path : s3://ecom-raw/validated/2026-11-18/
glue_transform_orders --target_path : s3://ecom-curated/orders/load_date=2026-11-18/
load_redshift         COPY FROM  : s3://ecom-curated/orders/load_date=2026-11-18/
load_redshift         statements : 8 (DELETE, COPY, DROP, CREATE TEMP, INSERT x3, DELETE) in one transaction
'''

FILES['output/test_results.txt'] = r'''============================= test session starts ==============================
platform linux -- Python 3.13.18, pytest-9.1.1, pluggy-1.6.0 -- /usr/local/bin/python3
cachedir: .pytest_cache
rootdir: aws-ecommerce-pipeline
plugins: platformdirs-4.13.5, anyio-4.15.1
collecting ... collected 11 items

tests/test_validate_lambda.py::test_validate_header_ok PASSED            [  9%]
tests/test_validate_lambda.py::test_validate_header_missing PASSED       [ 18%]
tests/test_validate_lambda.py::test_good_file_goes_to_validated PASSED   [ 27%]
tests/test_validate_lambda.py::test_bad_header_goes_to_quarantine PASSED [ 36%]
tests/test_validate_lambda.py::test_empty_and_wrong_extension PASSED     [ 45%]
tests/test_transform_orders.py::test_columns_match_staging_order PASSED  [ 54%]
tests/test_transform_orders.py::test_bad_rows_removed PASSED             [ 63%]
tests/test_transform_orders.py::test_dedup_keeps_latest PASSED           [ 72%]
tests/test_transform_orders.py::test_cleaning_and_line_total PASSED      [ 81%]
tests/test_quality_checks.py::test_every_check_has_one_select PASSED     [ 90%]
tests/test_quality_checks.py::test_load_sql_has_no_truncate_or_manual_transaction PASSED [100%]

============================= 11 passed in 33.80s ==============================
'''

FILES['output/test_results_dag.txt'] = r'''============================= test session starts ==============================
platform linux -- Python 3.11.18, pytest-8.3.4, pluggy-1.5.0 -- /tmp/af/bin/python
cachedir: .pytest_cache
rootdir: aws-ecommerce-pipeline
plugins: anyio-4.7.0, time-machine-2.16.0
collecting ... collected 2 items

tests/test_dag.py::test_dag_loads_and_order PASSED                       [ 50%]
tests/test_dag.py::test_parse_checks PASSED                              [100%]

============================== 2 passed in 3.18s ===============================
'''

FILES['output/transform_summary.txt'] = r'''Input rows : 9
Output rows: 4
Dropped    : 5 (zero qty, bad date, negative price, missing order_id, older duplicate)
'''

FILES['output/transformed_orders_sample.csv'] = r'''order_id,order_ts,customer_id,product_id,quantity,unit_price,currency,order_status,line_total,source_file
O1001,2026-11-18 09:15:00,C001,P100,2,19.99,USD,placed,39.98,
O1001,2026-11-18 09:15:00,C001,P200,1,5.50,USD,placed,5.50,
O1002,2026-11-18 11:45:00,C002,P100,3,19.99,USD,shipped,59.97,
O1006,2026-11-18 15:20:00,C006,P500,4,12.25,EUR,delivered,49.00,
'''

FILES['.env.example'] = r'''# Copy to .env (never commit .env). Values come from `terraform output`.
AWS_REGION=us-east-1
RAW_BUCKET=ecom-pipeline-dev-raw-123456789012
CURATED_PATH=s3://ecom-pipeline-dev-curated-123456789012/orders/
GLUE_JOB_NAME=ecom-pipeline-dev-transform-orders
REDSHIFT_IAM_ROLE=arn:aws:iam::123456789012:role/ecom-pipeline-dev-redshift-s3-read
'''

FILES['.gitignore'] = r'''# Python
__pycache__/
*.py[cod]
*.egg-info/
.venv/
venv/
.pytest_cache/
spark-warehouse/
metastore_db/
derby.log

# Env / secrets
.env
*.pem
credentials*
.aws/
terraform.tfvars

# Airflow
logs/
airflow.db
airflow.cfg
unittests.cfg

# Terraform / build
.terraform/
*.tfstate*
crash.log
terraform/build/
dist/
build/
*.zip

# OS / IDE
.DS_Store
.idea/
.vscode/
'''

FILES['README.md'] = r'''# AWS E-commerce Data Pipeline

Batch pipeline that validates raw order files, transforms them with Glue (PySpark),
loads a Redshift star schema, and runs data quality checks, orchestrated by Airflow.
Infrastructure is defined in Terraform.

## Architecture

![End-to-end architecture](docs/images/architecture.svg)

### Design decisions
- **One trigger path.** The Lambda only validates and routes files; Airflow is the only thing
  that starts Glue. So each file is processed once.
- **Batches by arrival date.** Files are grouped by the day they arrived, not by the order date
  inside them, so late or back-dated orders are never skipped.
- **Idempotent re-runs.** Glue overwrites `load_date=<ds>/`, and Redshift upserts with
  delete + insert in a single transaction, so re-running a day gives the same result.
- **Dedup twice.** Glue keeps the latest version of each `(order_id, product_id)`. The load SQL
  dedupes staging again before it touches the fact table.

### Airflow DAG

![Airflow DAG tasks](docs/images/airflow_dag.svg)

### Redshift data model

![Redshift star schema](docs/images/star_schema.svg)

## Project layout

| Path | Purpose |
|------|---------|
| `lambda/validate_and_trigger.py` | Validates CSVs (extension, size, header) and routes them to `validated/` or `quarantine/` |
| `glue/transform_orders.py` | Cleans, types, dedupes; writes Parquet in staging column order |
| `sql/redshift_schema.sql` | Staging, dimension and fact table DDL |
| `sql/load_redshift.sql` | COPY + dedupe + upsert (one transaction) |
| `quality/checks.sql` | DQ queries; each returns a failing-row count |
| `dags/ecommerce_pipeline_dag.py` | Airflow orchestration |
| `iam/*.json` | Least-privilege IAM policies (Terraform fills `${...}` placeholders) |
| `terraform/` | S3, Lambda + trigger, Glue job, IAM, Redshift Serverless |
| `sample_data/` | Sample good/bad files for testing |
| `docs/` | README diagrams and the script that generates them |
| `output/` | Real results from running the pipeline code on the sample data |
| `tests/` | pytest: Lambda (moto), Glue transform (local Spark), SQL checks, DAG |

## Input format (CSV)

`order_id, order_ts, customer_id, product_id, quantity, unit_price, currency, order_status`

## Deploy

```bash
# 1. Infrastructure
cd terraform
cp terraform.tfvars.example terraform.tfvars    # set redshift_admin_password
terraform init && terraform apply
terraform output                                # values for .env / Airflow

# 2. Redshift tables: run sql/redshift_schema.sql in the Redshift query editor

# 3. Airflow (MWAA or self-hosted)
#    - pip install -r requirements.txt
#    - copy dags/, sql/ and quality/ side by side into the DAGs folder
#    - connections: aws_default, redshift_default
#    - env vars from .env.example (values from terraform output)
#    - attach the airflow_policy_arn output to the MWAA execution role

# 4. Test it
aws s3 cp sample_data/orders_sample.csv s3://<raw_bucket>/incoming/
#    -> moved to validated/<today>/ ; the next day's DAG run processes it
#    (or trigger the DAG manually with logical date = today)
```

## Run tests locally

```bash
pip install -r requirements-dev.txt     # pyspark needs Java 8/11/17
pytest -v
```
`tests/test_dag.py` is skipped unless Airflow is installed.

## Data quality checks

Empty staging, null keys, duplicate order lines, non-positive quantity, line-total mismatch,
future-dated orders, and orphan customer/product keys.

## Sample output

![Glue transform on the sample data](docs/images/data_cleaning.svg)

See [`output/`](output/) for the full results: test runs, the Lambda routing log, and the
transformed CSV.

## Regenerate this project

```bash
python setup_project.py
```
'''

FILES['requirements-dev.txt'] = r'''# Local testing
pytest>=8.0
boto3>=1.34
moto[s3]>=5.0
pyspark>=3.3      # Glue 4.0 runs Spark 3.3; any 3.3+ works for the unit tests (needs Java)
'''

FILES['requirements.txt'] = r'''# Airflow environment (MWAA or self-hosted Airflow 2.7+)
apache-airflow-providers-amazon>=8.16
apache-airflow-providers-common-sql>=1.10
boto3>=1.34
'''


def main():
    for rel_path, content in FILES.items():
        full = os.path.join(PROJECT, rel_path)
        os.makedirs(os.path.dirname(full) or ".", exist_ok=True)
        with open(full, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)
        print(f"created {full}")

    zip_name = f"{PROJECT}.zip"
    with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel_path in FILES:
            zf.write(os.path.join(PROJECT, rel_path), arcname=os.path.join(PROJECT, rel_path))
        zf.write(os.path.abspath(__file__), arcname=os.path.join(PROJECT, "setup_project.py"))
    print(f"\nzipped -> {zip_name} ({len(FILES) + 1} files)")


if __name__ == "__main__":
    main()

"""
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

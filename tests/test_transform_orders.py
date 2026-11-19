import os
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

import os

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

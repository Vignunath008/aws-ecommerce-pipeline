import os
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

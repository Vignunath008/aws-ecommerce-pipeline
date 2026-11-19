import os

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

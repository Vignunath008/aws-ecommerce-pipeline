# Sample output

Generated locally from `sample_data/` (no AWS account needed).

| File | What it shows |
|------|---------------|
| `test_results.txt` | pytest run: Lambda (moto S3), Glue transform (local Spark), SQL checks |
| `test_results_dag.txt` | pytest run of the Airflow DAG under Airflow 2.10 |
| `lambda_routing_log.txt` | Lambda routing good files to `validated/` and bad files to `quarantine/` |
| `transformed_orders_sample.csv` | Glue `transform()` output for `orders_sample.csv` (9 rows in → 4 clean rows out) |
| `transform_summary.txt` | Row counts and why rows were dropped |
| `rendered_dag_paths.txt` | S3 paths the DAG resolves for logical date 2026-11-18 |

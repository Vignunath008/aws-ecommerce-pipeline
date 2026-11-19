-- Load one day's curated Parquet into Redshift and upsert into analytics tables.
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

-- Data quality checks. Each block starts with "-- name: <check_name>" and
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

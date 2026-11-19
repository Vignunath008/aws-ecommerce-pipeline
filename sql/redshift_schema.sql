-- Redshift schema for the e-commerce pipeline
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

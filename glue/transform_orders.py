"""
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

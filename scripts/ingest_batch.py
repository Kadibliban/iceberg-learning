import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

def log_message(message):
    with open("/home/iceberg/ingestion.log", "a") as log_file:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"[{timestamp}] {message}\n")

spark = (
    SparkSession.builder
    .appName("IcebergIncrementalIngestion")
    .getOrCreate()
)

batch_file = f"/home/iceberg/data/{sys.argv[1]}"


log_message(f"Ingestion started for {sys.argv[1]}")

batch_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(batch_file)
)

batch_df = batch_df.withColumn(
    "customer_type",
    when(col("order_id") <= 6, "New").otherwise("Returning")
)

# Check for duplicate order IDs within the incoming batch
duplicate_count = (
    batch_df.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count > 0:
    raise ValueError("Data quality check failed: duplicate order_id found in batch.")

print("Data quality check passed: no duplicate order IDs.")

# Check for missing order IDs
null_order_id_count = batch_df.filter(col("order_id").isNull()).count()

if null_order_id_count > 0:
    raise ValueError("Data quality check failed: missing order_id found in batch.")

print("Data quality check passed: no missing order IDs.")

# Check for invalid quantities
invalid_quantity_count = batch_df.filter(col("quantity") <= 0).count()

if invalid_quantity_count > 0:
    raise ValueError("Data quality check failed: quantity must be greater than 0.")

print("Data quality check passed: all quantities are valid.")

# Check for missing prices
null_price_count = batch_df.filter(col("price").isNull()).count()

if null_price_count > 0:
    raise ValueError("Data quality check failed: missing price found in batch.")

print("Data quality check passed: no missing prices.")
log_message(f"Data quality checks passed for {sys.argv[1]}")

batch_df.createOrReplaceTempView("incoming_sales")

spark.sql("""
MERGE INTO demo.sales.sales_by_date AS target
USING incoming_sales AS source
ON target.order_id = source.order_id

WHEN MATCHED THEN UPDATE SET
    target.customer_name = source.customer_name,
    target.product = source.product,
    target.quantity = source.quantity,
    target.price = source.price,
    target.order_date = source.order_date,
    target.payment_method = source.payment_method,
    target.customer_type = source.customer_type

WHEN NOT MATCHED THEN INSERT (
    order_id,
    customer_name,
    product,
    quantity,
    price,
    order_date,
    payment_method,
    customer_type
)
VALUES (
    source.order_id,
    source.customer_name,
    source.product,
    source.quantity,
    source.price,
    source.order_date,
    source.payment_method,
    source.customer_type
)
""")

print("Batch successfully merged into Iceberg.")

print(
    f"Total records: "
    f"{spark.table('demo.sales.sales_by_date').count()}"
)

log_message(f"Ingestion completed successfully for {sys.argv[1]}")
spark.stop()
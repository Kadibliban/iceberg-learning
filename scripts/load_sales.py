from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IcebergSalesIngestion")
    .getOrCreate()
)

sales_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/iceberg/data/sales.csv")
)

print("Sales schema:")
sales_df.printSchema()

print(f"Total records: {sales_df.count()}")

sales_df.writeTo("demo.sales.sales_data").using("iceberg").createOrReplace()

print("Iceberg table created successfully.")

spark.stop()
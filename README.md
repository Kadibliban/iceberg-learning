# Apache Iceberg Data Engineering Learning Project

A hands-on data engineering project built to learn Apache Iceberg with Apache Spark, Docker, an Iceberg REST catalog, and S3-compatible object storage using RustFS.

The project demonstrates how to build and manage a reliable incremental data ingestion pipeline using Iceberg tables.

## Technologies Used

* Apache Iceberg
* Apache Spark
* Spark SQL
* Docker / Docker Compose
* Iceberg REST Catalog
* RustFS (S3-compatible object storage)
* Python
* Git / GitHub

## Project Objectives

This project was created to gain practical experience with:

* Creating and querying Iceberg tables
* Incremental data ingestion
* Schema evolution
* Time travel
* Snapshot management
* Rollbacks
* Table partitioning
* MERGE / UPSERT operations
* Idempotent data ingestion
* Data-quality validation
* Ingestion logging
* Audit tracking


## Project Structure

```text
iceberg-learning/
├── data/
│   ├── sales.csv
│   ├── sales_batch_2.csv
│   ├── sales_batch_3.csv
│   ├── sales_batch_4.csv
│   ├── sales_batch_5.csv
│   ├── sales_batch_6.csv
│   └── sales_bad*.csv
│
├── notebooks/
├── scripts/
│   ├── read_sales.py
│   ├── load_sales.py
│   └── ingest_batch.py
│
├── sql/
├── warehouse/
├── docker-compose.yml
├── .gitignore
└── README.md
```

### Main Components

**`data/`**
Contains the original sales dataset, incremental batches, and test datasets used for data-quality validation.

**`scripts/`**
Contains Python scripts for reading data, loading the initial dataset, and performing incremental/idempotent ingestion.

**`docker-compose.yml`**
Defines the Spark, Iceberg REST catalog, RustFS object storage, and bucket initialization services.

**`warehouse/`**
Local project warehouse directory. Iceberg table data in this setup is stored through the S3-compatible RustFS object store.

**`sql/`**
Contains SQL queries and exercises used during the project.

## Iceberg Features Demonstrated

### 1. Data Ingestion

Loaded CSV sales data into an Apache Iceberg table using Apache Spark.

The main table is:

```sql
demo.sales.sales_by_date
```

The project supports both initial loading and incremental batch ingestion.

### 2. Schema Evolution

Added new columns to existing Iceberg tables without recreating the tables.

Example:

```sql
ALTER TABLE demo.sales.sales_data
ADD COLUMNS (
    customer_type STRING
);
```

### 3. Time Travel

Queried historical versions of an Iceberg table using snapshot IDs.

```sql
SELECT *
FROM demo.sales.sales_data
VERSION AS OF 5775513673949800104;
```

This demonstrated the ability to access data as it existed at an earlier point in time.

### 4. MERGE / UPSERT

Used Iceberg `MERGE INTO` to update existing records and insert new records.

This was also used to make batch ingestion idempotent.

### 5. Partitioning

Created a date-partitioned Iceberg table:

```sql
PARTITIONED BY (days(order_date))
```

This organizes the data by order date and supports efficient filtering.

### 6. Rollback

Tested Iceberg snapshot rollback by returning the table to a previous snapshot.

This demonstrated recovery from an unwanted change.

### 7. Snapshot Maintenance

Tested snapshot expiration using:

```sql
CALL demo.system.expire_snapshots(...)
```

### 8. Data File Compaction

Tested Iceberg data-file rewriting using:

```sql
CALL demo.system.rewrite_data_files(...)
```

The dataset was too small to require file rewriting, so no files were rewritten.

### 9. Data Quality Validation

The ingestion pipeline validates incoming batches for:

* Duplicate order IDs
* Missing order IDs
* Invalid quantities
* Missing prices

Invalid batches are rejected before the `MERGE` operation.

### 10. Idempotent Ingestion

The ingestion pipeline uses `MERGE` based on `order_id`.

Rerunning the same batch does not create duplicate records.

### 11. Ingestion Logging

The pipeline records:

* Ingestion start
* Data-quality validation success
* Successful ingestion completion

Example:

```text
[2026-10-06 13:29:48] Ingestion started for sales_batch_6.csv
[2026-10-06 13:29:55] Data quality checks passed for sales_batch_6.csv
[2026-10-06 13:30:01] Ingestion completed successfully for sales_batch_6.csv
```

### 12. Audit Tracking

Created an Iceberg audit table:

```sql
demo.sales.ingestion_audit
```

The audit table tracks:

* Batch filename
* Total table records
* Ingestion timestamp
* Number of records in the batch
* Processing status

Current successful audit records include Batch 5 and Batch 6, with the table containing 34 records.


## Data Quality Testing

The ingestion pipeline was tested using intentionally invalid datasets to confirm that invalid records are rejected before they reach the Iceberg table.

### Tests Performed

| Test                 | Expected Result      | Result |
| -------------------- | -------------------- | ------ |
| Duplicate `order_id` | Reject batch         | PASS   |
| Missing `order_id`   | Reject batch         | PASS   |
| Quantity ≤ 0         | Reject batch         | PASS   |
| Missing price        | Reject batch         | PASS   |
| Valid batch          | Successfully ingest  | PASS   |
| Rerun existing batch | No duplicate records | PASS   |

### Example Validation

For duplicate order IDs, the pipeline performs a validation before the `MERGE`:

```python
duplicate_count = (
    batch_df.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count > 0:
    raise ValueError(
        "Data quality check failed: duplicate order_id found in batch."
    )
```

If validation fails, the ingestion stops before modifying the Iceberg table.

### Idempotency Test

Batch 6 was ingested more than once using the `MERGE`-based ingestion pipeline.

The final table remained at:

```text
34 records
```

This confirmed that rerunning an existing batch does not create duplicate records.

## How to Run the Project

### Prerequisites

Install:

* Docker Desktop
* Python
* Git
* VS Code

### 1. Clone the Repository

```bash
git clone <your-github-repository-url>
cd iceberg-learning
```

### 2. Start the Services

From the project directory:

```bash
docker compose up -d
```

This starts:

* Spark
* Iceberg REST Catalog
* RustFS object storage
* S3 bucket initialization

### 3. Check Running Containers

```bash
docker ps
```

The main services should include:

```text
spark-iceberg
iceberg-rest
object-store
```

### 4. Run the Initial Data Load

Copy the ingestion script into the Spark container:

```powershell
docker cp scripts\load_sales.py spark-iceberg:/home/iceberg/load_sales.py
```

Then run:

```powershell
docker exec spark-iceberg spark-submit /home/iceberg/load_sales.py
```

### 5. Run Incremental Ingestion

Copy the ingestion script:

```powershell
docker cp scripts\ingest_batch.py spark-iceberg:/home/iceberg/ingest_batch.py
```

Run an incremental batch:

```powershell
docker exec spark-iceberg spark-submit /home/iceberg/ingest_batch.py sales_batch_6.csv
```

The ingestion script performs data-quality validation and uses `MERGE` to update or insert records.

### 6. Query the Iceberg Table

```powershell
docker exec spark-iceberg spark-sql -e "SELECT * FROM demo.sales.sales_by_date;"
```

Check the total number of records:

```powershell
docker exec spark-iceberg spark-sql -e "SELECT COUNT(*) AS total_records FROM demo.sales.sales_by_date;"
```

### 7. Stop the Project

When finished:

```bash
docker compose down
```

## Notes

The project uses RustFS as S3-compatible object storage for the Iceberg warehouse.

The Iceberg REST catalog is used to manage table metadata and catalog operations.

For learning purposes, the project intentionally includes data-quality tests and recovery exercises such as rollback and time travel.


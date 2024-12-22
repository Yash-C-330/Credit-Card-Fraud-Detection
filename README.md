# Credit Card Fraud Detection Project

## Overview
This project aims to detect fraudulent transactions in real-time and batch processing scenarios using big data tools and techniques. The architecture combines streaming and batch pipelines to process transactions, update lookup tables, and determine fraudulence based on predefined rules.

## Project Architecture
### Components Used
1. **Amazon S3**:
   - Stores raw data files (CSV format).
   - Holds card member details, scores, and transactions.
2. **Databricks**:
   - Processes data using Spark.
   - Implements Delta Live Tables (DLTs) for data transformations.
3. **Apache Kafka**:
   - Streams new transaction data in real-time.
4. **Delta Lake**:
   - Manages transaction and lookup tables with ACID properties.
5. **SQL**:
   - Performs data cleaning, transformations, and aggregations.

### Workflow
1. **Raw Data Ingestion**:
   - Raw data is stored in S3 buckets as CSV files.
   - Data includes card transactions, member details, and member scores.
2. **Batch Processing**:
   - Batch loads raw data into Databricks tables using Delta Lake.
   - A lookup table is created to store aggregated statistics.
3. **Streaming Processing**:
   - Kafka streams new transactions to Databricks.
   - Transactions are processed and appended to existing Delta tables.
4. **Fraud Detection**:
   - Rules are applied to determine if a transaction is fraudulent.
   - Outputs are stored in a separate fraud detection table.

## Data Pipeline Steps

### 1. Data Cleaning and Preprocessing
- **Remove Duplicates**:
  ```sql
  CREATE OR REPLACE TABLE credit_card_db.silver_transactions AS
  SELECT DISTINCT *
  FROM credit_card_db.raw_transactions;
  ```

- **Add Primary Key**:
  A combination of `card_id` and `transaction_dt` is created to ensure uniqueness.
  ```sql
  ALTER TABLE credit_card_db.silver_transactions ADD COLUMN primary_key STRING;
  UPDATE credit_card_db.silver_transactions
  SET primary_key = CONCAT(card_id, '_', transaction_dt);
  ```

### 2. Lookup Table Creation
- Aggregate transaction statistics per `card_id`:
  ```sql
  WITH cte_rownum AS (
      SELECT
          card_id,
          amount,
          member_id,
          transaction_dt,
          post_code,
          ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY transaction_dt DESC) AS rownum
      FROM credit_card_db.silver_transactions
  ),
  processed_data AS (
      SELECT
          card_id,
          amount,
          c.member_id,
          m.score,
          c.transaction_dt,
          c.post_code,
          STDDEV(amount) OVER (PARTITION BY card_id ORDER BY transaction_dt DESC) AS std
      FROM cte_rownum c
      INNER JOIN credit_card_db.member_score m ON c.member_id = m.member_id
      WHERE rownum <= 10
  )
  SELECT
      card_id,
      member_id,
      ROUND(AVG(amount) + 3 * MAX(std), 0) AS UCL,
      MAX(score) AS score,
      MAX(transaction_dt) AS last_txn_time,
      MAX(post_code) AS last_txn_zip
  FROM processed_data
  GROUP BY card_id, member_id;
  ```

### 3. Data Streaming with Kafka
- Stream data from Kafka into Databricks Delta tables.
- Use Autoloader for efficient streaming.

### 4. Fraud Detection Rules
- Flag transactions exceeding `UCL` values from the lookup table.
- Cross-check member scores and transaction patterns.

## Technologies Used
- **Databricks**: For data engineering and ML pipelines.
- **Delta Lake**: To ensure ACID compliance and versioning.
- **Apache Kafka**: For real-time streaming.
- **Amazon S3**: As the raw data storage layer.
- **PySpark/SQL**: For data transformations and aggregations.

## Installation and Setup
### Prerequisites
- Databricks account.
- S3 bucket with raw data files.
- Kafka cluster for streaming.

### Steps
1. Clone this repository:
   ```bash
   git clone <repository-url>
   ```
2. Configure S3 bucket permissions for Databricks.
3. Set up your Databricks workspace with:
   - Unity Catalog (if applicable).
   - Delta Live Tables.
4. Create required databases:
   ```sql
   CREATE DATABASE IF NOT EXISTS credit_card_db;
   ```
5. Deploy and test the pipeline notebooks.

## Usage
- Upload raw data files to S3 buckets.
- Run the batch pipelines for initial data processing.
- Enable the Kafka streaming pipeline for real-time transaction ingestion.
- Query the fraud detection table for results.

## Future Enhancements
- Implement ML models for fraud detection.
- Use AWS RDS or DynamoDB for faster lookup table updates.
- Introduce a dashboard for monitoring fraud metrics.

## License
This project is licensed under the MIT License. See `LICENSE` for details.

## Contributing
Contributions are welcome! Please fork the repository and submit a pull request.

---

Feel free to reach out for any questions or suggestions!



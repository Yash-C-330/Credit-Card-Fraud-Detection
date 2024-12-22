# Credit-Card-Fraud-Detection

Overview

This project aims to detect fraudulent transactions in real-time and batch processing scenarios using big data tools and techniques. The architecture combines streaming and batch pipelines to process transactions, update lookup tables, and determine fraudulence based on predefined rules.

Project Architecture

Components Used

Amazon S3:

Stores raw data files (CSV format).

Holds card member details, scores, and transactions.

Databricks:

Processes data using Spark.

Implements Delta Live Tables (DLTs) for data transformations.

Apache Kafka:

Streams new transaction data in real-time.

Delta Lake:

Manages transaction and lookup tables with ACID properties.

SQL:

Performs data cleaning, transformations, and aggregations.

Workflow

Raw Data Ingestion:

Raw data is stored in S3 buckets as CSV files.

Data includes card transactions, member details, and member scores.

Batch Processing:

Batch loads raw data into Databricks tables using Delta Lake.

A lookup table is created to store aggregated statistics.

Streaming Processing:

Kafka streams new transactions to Databricks.

Transactions are processed and appended to existing Delta tables.

Fraud Detection:

Rules are applied to determine if a transaction is fraudulent.

Outputs are stored in a separate fraud detection table.

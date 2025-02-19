-- Databricks notebook source
-- MAGIC %run /Workspace/Users/y.hanumant.chaudhari@accenture.com/Credit_card_fraud_detection/Mounting_AWS

-- COMMAND ----------


show current database

-- COMMAND ----------

CREATE DATABASE credit_card_db

-- COMMAND ----------

USE credit_card_db

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS credit_card_db.card_details( card_id LONG, member_id LONG, member_joining_dt TIMESTAMP, card_purchase_dt DATE, country STRING, city STRING)

-- COMMAND ----------

-- COPY INTO credit_card_db.card_details
-- FROM 's3://credit-card-data-dbc/raw-data/card-members.csv'
-- FILEFORMAT = CSV
-- FORMAT_OPTIONS ('header' = 'false', 'inferSchema' = 'true')
-- COPY_OPTIONS('mergeSchema' = 'true');


-- SELECT COUNT(*) AS COUNT FROM credit_card_db.card_details;

-- COMMAND ----------

SELECT * FROM credit_card_db.card_details LIMIT 10;

-- COMMAND ----------

DROP TABLE IF EXISTS credit_card_db.card_details;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema = "card_id LONG, member_id LONG, member_joining_dt TIMESTAMP, card_purchase_dt STRING, country STRING, city STRING"
-- MAGIC df = spark.read.csv("s3://credit-card-data-dbc/raw-data/card-members.csv", schema=schema, header=False)
-- MAGIC df.write.format("delta").mode("overwrite").saveAsTable("credit_card_db.card_details")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema_m= "member_id LONG, score FLOAT"
-- MAGIC df_m = spark.read.csv("s3://credit-card-data-dbc/raw-data/member-score.csv", schema=schema_m, header=False)
-- MAGIC df_m.write.format("delta").mode("overwrite").saveAsTable("credit_card_db.member_score")

-- COMMAND ----------

select distinct count(*) from credit_card_db.member_score;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema_t = "card_id LONG, member_id LONG, amount FLOAT, post_code LONG, pos_id LONG, transaction_dt TIMESTAMP, status STRING"
-- MAGIC df_t = spark.read.csv("s3://credit-card-data-dbc/raw-data/card-transactions.csv", schema=schema_t, header=False, timestampFormat="dd-MM-yyyy H:mm:ss")
-- MAGIC df_t.write.format("delta").mode("overwrite").saveAsTable("credit_card_db.transactions")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Cleaning Transactions Table**
-- MAGIC
-- MAGIC There are duplicates present in transactions table. Remove duplicates and create silver transactions table

-- COMMAND ----------

SELECT COUNT(card_id, transaction_dt)  AS COUNT FROM credit_card_db.transactions

-- COMMAND ----------

SELECT COUNT(DISTINCT card_id, transaction_dt)  AS COUNT FROM credit_card_db.transactions

-- COMMAND ----------

select * from credit_card_db.transactions

-- COMMAND ----------

SELECT card_id, transaction_dt, COUNT(*) AS count
FROM credit_card_db.transactions
GROUP BY card_id, transaction_dt
HAVING COUNT(*) > 1;

-- COMMAND ----------

select * from credit_card_db.transactions
where card_id = 343962656588624 and transaction_dt = '2017-12-19T23:25:07.000+00:00'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Load the existing card_transaction table
-- MAGIC card_transaction_df = spark.table("credit_card_db.transactions")
-- MAGIC
-- MAGIC # Remove duplicates based on primary key (card_id and transaction_dt)
-- MAGIC cleaned_df = card_transaction_df.dropDuplicates(["card_id", "transaction_dt"])
-- MAGIC
-- MAGIC # Write the cleaned data to a new Delta table called silver_transactions
-- MAGIC cleaned_df.write.format("delta").mode("overwrite").saveAsTable("credit_card_db.silver_transactions")
-- MAGIC
-- MAGIC print("Data cleaning complete. 'silver_transactions' table created.")

-- COMMAND ----------

select * from credit_card_db.silver_transactions limit 10;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC lookup_df = spark.sql("""
-- MAGIC     WITH cte_rownum AS (
-- MAGIC         SELECT 
-- MAGIC             card_id, 
-- MAGIC             amount, 
-- MAGIC             member_id, 
-- MAGIC             transaction_dt, 
-- MAGIC             post_code,
-- MAGIC             ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY transaction_dt DESC) AS rownum
-- MAGIC         FROM credit_card_db.silver_transactions
-- MAGIC     ),
-- MAGIC     processed_data AS (
-- MAGIC         SELECT 
-- MAGIC             card_id, 
-- MAGIC             amount, 
-- MAGIC             c.member_id, 
-- MAGIC             m.score, 
-- MAGIC             c.transaction_dt, 
-- MAGIC             c.post_code, 
-- MAGIC             STDDEV(amount) OVER (PARTITION BY card_id ORDER BY (SELECT 1) DESC) AS std
-- MAGIC         FROM cte_rownum c
-- MAGIC         INNER JOIN credit_card_db.member_score m ON c.member_id = m.member_id
-- MAGIC         WHERE rownum <= 10
-- MAGIC     )
-- MAGIC     SELECT 
-- MAGIC         card_id, 
-- MAGIC         member_id, 
-- MAGIC         ROUND(AVG(amount) + 3 * MAX(std), 0) AS UCL, 
-- MAGIC         MAX(score) AS score, 
-- MAGIC         MAX(transaction_dt) AS last_txn_time, 
-- MAGIC         MAX(post_code) AS last_txn_zip
-- MAGIC     FROM processed_data
-- MAGIC     GROUP BY card_id, member_id
-- MAGIC """)
-- MAGIC
-- MAGIC lookup_df.write.format("delta").mode("overwrite").saveAsTable("credit_card_db.lookup_table")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(lookup_df)

-- COMMAND ----------

select * from credit_card_db.lookup_table

-- COMMAND ----------

WITH cte_rownum AS (
    SELECT 
        card_id, 
        amount, 
        member_id, 
        transaction_dt, 
        post_code,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY transaction_dt DESC) AS rownum
    FROM credit_card_db.silver_transactions
)
SELECT * FROM cte_rownum LIMIT 10;

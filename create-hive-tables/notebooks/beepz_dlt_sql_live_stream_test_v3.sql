-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE reviews_bronze
AS SELECT
  *
FROM
  STREAM read_files(
    'dbfs:/databricks-datasets/amazon/data20K',
    format => 'parquet',
    header => true,
    mode => 'DROPMALFORMED'
  )

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE reviews_silver
AS SELECT
  *
FROM STREAM(LIVE.reviews_bronze)
where rating > 1.0

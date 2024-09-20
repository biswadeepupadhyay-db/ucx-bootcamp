-- Databricks notebook source
USE CATALOG hive_metastore

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS amazon_data20K_schema
COMMENT 'Schema for Amazon datasets from DBFS'
MANAGED LOCATION 'dbfs:/user/hive/warehouse/amazon_data20K_schema.db';


-- COMMAND ----------

use schema amazon_data20K_schema;

-- COMMAND ----------

CREATE OR REPLACE TABLE delta_reviews_managed
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/amazon_data20K_schema.db/delta_reviews_managed'
COMMENT 'Managed Delta table with DBFS Root'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
AS
SELECT *
FROM parquet.`dbfs:/databricks-datasets/amazon/data20K/`;



-- COMMAND ----------

CREATE OR REPLACE TABLE delta_ratings_managed
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/amazon_data20K_schema.db/delta_ratings_managed'
COMMENT 'Managed Ratings Delta table with DBFS Root'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
AS
SELECT *
FROM
  read_files(
    'dbfs:/databricks-datasets/cs110x/ml-20m/data-001/ratings.csv',
    format => 'csv',
    header => true,
    sep => ',',
    mode => 'DROPMALFORMED'
  );

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS amazon_data20K_managed
COMMENT 'Schema for Amazon datasets from DBFS'

-- COMMAND ----------

use schema amazon_data20K_managed

-- COMMAND ----------

CREATE OR REPLACE TABLE delta_reviews_managed
USING DELTA
COMMENT 'Managed Delta table with DBFS Root'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
AS
SELECT *
FROM parquet.`dbfs:/databricks-datasets/amazon/data20K/`;

-- COMMAND ----------

CREATE OR REPLACE TABLE delta_ratings_managed
USING DELTA
COMMENT 'Managed Ratings Delta table with DBFS Root'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
AS
SELECT *
FROM
  read_files(
    'dbfs:/databricks-datasets/cs110x/ml-20m/data-001/ratings.csv',
    format => 'csv',
    header => true,
    sep => ',',
    mode => 'DROPMALFORMED'
  );

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS amazon_data20K_external
COMMENT 'Schema for Amazon datasets from ABFSS'
LOCATION 'abfss://ucx-bootcamp-lakehouse@saucxbootcamp.dfs.core.windows.net/amazon_data20K_external.db';


-- COMMAND ----------

use schema amazon_data20K_external

-- COMMAND ----------

CREATE OR REPLACE TABLE amazon_data20K_external.delta_reviews_external
USING DELTA
LOCATION 'abfss://ucx-bootcamp-lakehouse@saucxbootcamp.dfs.core.windows.net/amazon_data20K_external.db/delta_reviews_external'
COMMENT 'External Delta table on ABFSS for Amazon dataset'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
AS
SELECT *
FROM parquet.`dbfs:/databricks-datasets/amazon/data20K/`;

-- COMMAND ----------

CREATE OR REPLACE TABLE amazon_data20K_external.delta_ratings_external
USING DELTA
LOCATION 'abfss://ucx-bootcamp-lakehouse@saucxbootcamp.dfs.core.windows.net/amazon_data20K_external.db/delta_ratings_external'
COMMENT 'External Delta table Ratings on ABFSS for Amazon dataset'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
AS
SELECT *
FROM
  read_files(
    'dbfs:/databricks-datasets/cs110x/ml-20m/data-001/ratings.csv',
    format => 'csv',
    header => true,
    sep => ',',
    mode => 'DROPMALFORMED'
  );

-- COMMAND ----------



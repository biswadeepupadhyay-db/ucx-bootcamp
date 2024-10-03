import os
import sys
import json
from typing import Optional, List, Dict

# def get_table_commands(external_location_uri: str) -> Optional[List[str]]:
#
#     return [
#         '''USE CATALOG hive_metastore;''',
#         """CREATE DATABASE IF NOT EXISTS amazon_data20K_schema
#            COMMENT 'Schema for Amazon datasets from DBFS'
#            MANAGED LOCATION 'dbfs:/user/hive/warehouse/amazon_data20K_schema.db';"""
#     ]

def get_table_ddl_commands(external_location_uri: str) -> Optional[str]:
    return f"""
    USE CATALOG hive_metastore;
    
    CREATE DATABASE IF NOT EXISTS product_reviews
    COMMENT 'Schema for Amazon datasets from DBFS'
    MANAGED LOCATION 'dbfs:/user/hive/warehouse/product_reviews.db';
    
    USE SCHEMA product_reviews;
    
    CREATE OR REPLACE TABLE delta_reviews_managed
    USING DELTA
    LOCATION 'dbfs:/user/hive/warehouse/product_reviews.db/delta_reviews_managed'
    COMMENT 'Managed Delta table with DBFS Root'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
    AS
    SELECT *
    FROM parquet.`dbfs:/databricks-datasets/amazon/data20K/`;
    
    CREATE OR REPLACE TABLE delta_ratings_managed
    USING DELTA
    LOCATION 'dbfs:/user/hive/warehouse/product_reviews.db/delta_ratings_managed'
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
      
    CREATE DATABASE IF NOT EXISTS product_reviews_managed
    COMMENT 'Schema for Amazon datasets from DBFS';
    
    USE SCHEMA product_reviews_managed;
    
    CREATE OR REPLACE TABLE delta_reviews_managed
    USING DELTA
    COMMENT 'Managed Delta table with DBFS Root'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
    AS
    SELECT *
    FROM parquet.`dbfs:/databricks-datasets/amazon/data20K/`;
    
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
    
    CREATE DATABASE IF NOT EXISTS product_reviews_external
    COMMENT 'Schema for Amazon datasets from ABFSS'
    LOCATION '{external_location_uri.rstrip('/')}/delta_tables/product_reviews_external.db';
    
    USE SCHEMA product_reviews_external;
    
    CREATE OR REPLACE TABLE product_reviews_external.delta_reviews_external
    USING DELTA
    LOCATION '{external_location_uri.rstrip('/')}/delta_tables/product_reviews_external.db/delta_reviews_external'
    COMMENT 'External Delta table on ABFSS for Amazon dataset'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = true, 'delta.autoOptimize.autoCompact' = true)
    AS
    SELECT *
    FROM parquet.`dbfs:/databricks-datasets/amazon/data20K/`;
    
    CREATE OR REPLACE TABLE product_reviews_external.delta_ratings_external
    USING DELTA
    LOCATION '{external_location_uri.rstrip('/')}/delta_tables/product_reviews_external.db/delta_ratings_external'
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
      
    """

def get_table_names() -> Dict[str,List[str]]:
    return {
        "product_reviews" : ['delta_ratings_managed', 'delta_reviews_managed'],
        "product_reviews_external" : ['delta_ratings_external', 'delta_ratings_external'],
        "product_reviews_managed" : ['delta_reviews_managed', 'delta_reviews_managed']
    }

def table_grants_commands(group_names: List[str]) -> str:
    table_and_schema_names = get_table_names()
    sql_commands = ""
    catalog_name = 'hive_metastore'
    schemas = []
    tables = []
    for k, v in table_and_schema_names.items():
        _schema = f"{catalog_name}.{k}"
        schemas.append(_schema)
        _tables = [f"{_schema}.{table}" for table in v]
        tables.append(_tables)

    grant_resources = zip(group_names, schemas, tables)

    for group_name, schema, tables in grant_resources:
        sql_commands += f"""
        \nGRANT MODIFY, SELECT ON ANY FILE to {group_name};
        
        GRANT USAGE, READ_METADATA, SELECT ON CATALOG {catalog_name} to {group_name};
        
        GRANT CREATE, MODIFY, READ_METADATA, SELECT, USAGE ON SCHEMA {schema} to {group_name};
        """

        for table in tables:
            sql_commands += f"\nGRANT MODIFY, SELECT, READ_METADATA ON TABLE {table} to {group_name};"

    return sql_commands





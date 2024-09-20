# ucx-bootcamp
### Utilities for UCX Bootcamp

#### Pipeline settings for External Materialized Views

```json
{
    "id": "671c22f8-b793-4c0b-9ca1-825693b54753",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "spark_conf": {
                "fs.azure.account.key.saucxbootcamp.dfs.core.windows.net": "{{secrets/ucx-bootcamp/fs_azure_key}}"
            },
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "CURRENT",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Users/biswadeep.upadhyay@databricks.com/ucx_mvp/dlt_viking_mvp/beepz_dlt_sql_hms_live_test_v2"
            }
        }
    ],
    "name": "ucx-bootcamp-dlt-live-external-v1",
    "edition": "ADVANCED",
    "storage": "abfss://ucx-bootcamp-lakehouse@saucxbootcamp.dfs.core.windows.net/data/",
    "target": "dlt_external",
    "data_sampling": false
}
```

#### Pipeline settings for External Streaming Tables

```json
{
    "id": "06a26066-d7e1-42cc-983e-09edcffdd00b",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "spark_conf": {
                "fs.azure.account.key.saucxbootcamp.dfs.core.windows.net": "{{secrets/ucx-bootcamp/fs_azure_key}}"
            },
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "CURRENT",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Users/biswadeep.upadhyay@databricks.com/ucx_mvp/dlt_viking_mvp/beepz_dlt_sql_live_stream_test_v3"
            }
        }
    ],
    "name": "ucx-bootcamp-dlt-stream-external-v1",
    "edition": "ADVANCED",
    "storage": "abfss://ucx-bootcamp-lakehouse@saucxbootcamp.dfs.core.windows.net/data-v2/dlt-external-stream",
    "target": "dlt_external_stream",
    "data_sampling": false
}
```

#### Pipeline settings for Managed Materialized Views

```json
{
    "id": "4d583ac8-7151-40fd-b324-efa5bff0db48",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "CURRENT",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Users/biswadeep.upadhyay@databricks.com/ucx_mvp/dlt_viking_mvp/beepz_dlt_sql_hms_live_test_v2"
            }
        }
    ],
    "name": "ucx-bootcamp-dlt-live-managed-v1",
    "edition": "ADVANCED",
    "storage": "dbfs:/pipelines/4d583ac8-7151-40fd-b324-efa5bff0db48",
    "target": "dlt_managed",
    "data_sampling": false
}
```

#### Pipeline settings for Managed Streaming Tables

```json
{
    "id": "67ffd14d-0e8b-4ab8-a464-b46322822cea",
    "pipeline_type": "WORKSPACE",
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "CURRENT",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Users/biswadeep.upadhyay@databricks.com/ucx_mvp/dlt_viking_mvp/beepz_dlt_sql_live_stream_test_v3"
            }
        }
    ],
    "name": "ucx-bootcamp-dlt-stream-managed-v1",
    "edition": "ADVANCED",
    "storage": "dbfs:/pipelines/67ffd14d-0e8b-4ab8-a464-b46322822cea",
    "target": "dlt_managed_stream",
    "data_sampling": false
}
```
#### Notebook Links for the above pipelines

* [Materialized Views](https://github.com/biswadeepupadhyay-db/ucx-bootcamp/blob/main/create-hive-tables/notebooks/beepz_dlt_sql_hms_live_test_v2.sql)
* [Streaming Tables](https://github.com/biswadeepupadhyay-db/ucx-bootcamp/blob/main/create-hive-tables/notebooks/beepz_dlt_sql_live_stream_test_v3.sql)

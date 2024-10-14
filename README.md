# ucx-bootcamp
### Utilities for UCX Bootcamp

This repo contains source code for creating Databricks Workspace on Azure & AWS. It also uses `python-sdk`
to deploy legacy hive resources on your workspace. 

[Bootcamp Guidebook](https://docs.google.com/document/d/1U8Rann-ahYHZq5n2PTxMriIw_WB_N4B-6Z7AyTuZ6GA/edit?pli=1&tab=t.0#heading=h.6c8zhcifwryl)


#### Prerequisites

1. Make sure you have `Account Admin` privileges on both your Cloud (AWS/Azure) & Databricks Account Console.
   1. [Databricks Account Console on Azure](https://accounts.azuredatabricks.net/)
   2. [Databricks Account Console on AWS](https://accounts.cloud.databricks.com)
2. Get in touch with your cloud administrator for any elevated privilege you might need.
3. You may use this utility for deploying either both an Azure Workspace & Hive resources or in case you already have
a workspace deployed, you may still use this utility to deploy just legacy hive resources on your Azure Databricks
workspace.
4. To run this utility you need to have following installed on your local machine.
   1. [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html)
   2. [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-macos)
   3. [Terraform CLI](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)
   4. [Python SDK for Databricks](https://docs.databricks.com/en/dev-tools/sdk-python.html)

#### How to run this utility on your localhost

1. Once you take care of all the prerequisites mentioned above, clone this repo to your local machine.
2. Go to the folder `./ucx-bootcamp`
3. Run the command : `python3 deploy_ws_resources.py`



###### Inputs needed by this script for deploying a workspace on Azure:

1. Azure Tenant ID. The script uses azure-cli to authenticate using your tenant ID. Once authenticated 
successfully, you can select the subscription of your choice where you are planning to deploy your workspace
resources.
2. Azure Region. Default : centralus
3. Deployment keyword identifier. This string will be present in your workspace name to uniquely identify your
resources deployed on Azure. Default : ucxbootcamp


* Once the Workspace deployment is done, the utility moves to the second section where it asks for details
to deploy legacy hive resources on your workspace
* If you already have a workspace deployed, you can have this script move to this second section directly by 
providing `no` when asked `Do you want to deploy a Workspace on Azure?[yes/no]` by the script.


###### Inputs needed by this script for deploying Hive resources on your Azure Databricks:

1. Your Email ID (username) that you use to login to Databricks.
2. Azure Databricks Workspace URL.
3. A service principal client-id and corresponding client-secret. The SDK will use this SP to authenticate
to your Workspace and create hive resources. make sure your SP has the following privileges.
   1. Cluster creation permission.
   2. Your SP must be a member of Workspace Admins system group.
   3. Your SP should have SELECT & MODIFY Grants to ANY FILE. You may use the below SQL query from a notebook
to grant this permission to your SP.
``GRANT SELECT, MODIFY ON ANY FILE TO `your-client-id`;``
4. The script will also create hive external tables on ADLS. Hence, make sure you already have a 
storage account and a container created. The Script expects you to provide a `storage-account` name &
a complete `abfss` path where you plan to store the data for your hive external tables.
5. The Databricks Notebook runtime context requires an `fs-azure-key` to gain access to your Azure container for 
creating external tables. You need to create one for your Azure storage account and pass that as input 
to the script. The script will create a Databricks-managed Secret Scope on your workspace to store the 
secret value. You are expected to give `scope-name`, `secret-key` & `secret-value (fs azure key value)`
as input.

#### Legacy Hive Resources

This script creates the following resources:
1. Secret Scope
2. A General Purpose Cluster
3. A SQL Warehouse
4. Hive tables
   1. Managed (DBFS root)
   2. External (on ADLS gen2)
   3. Streaming Managed tables
   4. External Materialized Views.
5. Workspace-level local groups
6. Grants on Catalog, Schemas, and Tables assigned to workspace groups.


#### Notebooks used for creating pipelines

* [Materialized Views](https://github.com/biswadeepupadhyay-db/ucx-bootcamp/blob/main/create-hive-tables/notebooks/beepz_dlt_sql_hms_live_test_v2.sql)
* [Streaming Tables](https://github.com/biswadeepupadhyay-db/ucx-bootcamp/blob/main/create-hive-tables/notebooks/beepz_dlt_sql_live_stream_test_v3.sql)

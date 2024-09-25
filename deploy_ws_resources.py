import os
import json
import datetime
from typing import List, Dict, Tuple, Optional


from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam, catalog
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    Channel,
    ChannelName,
    SpotInstancePolicy
)
from databricks.sdk.service.compute import (
    AutoScale,
    DataSecurityMode,
    RuntimeEngine,
    Language
)
from databricks.sdk.service.pipelines import (
    PipelineCluster,
    PipelineClusterAutoscale,
    PipelineClusterAutoscaleMode,
    PipelineLibrary,
    NotebookLibrary
)

CONFIG_PATH = './config/resource_setup.json'
DEPLOYSTATE_PATH = './.deploy_state.json'
# Deploy State Schema:
# {
#     "secret_scope_name" : None,
#     "secret_key" : None,
#     "cluster_id" : None,
#     "warehouse_id" : None,
#     "hive_tables" : None,
#     "dlt_tables" : None,
#     "groups" : None,
#     "grants" : None
# }

class DatabricksException(Exception):
    """Raise this for any custom exceptions."""

def read_json_file(file_path: str) -> dict:
    """
    Reads a JSON file and returns its contents as a Python object.
    :param file_path: The path to the JSON file
    :return: The contents of the JSON file as a Python Dictionary
    """
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Creating deploystate at {file_path}")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file {file_path}")
        print(f"JSON decode error: {str(e)}")
        raise

def write_deploy_state(data):
    """
    writes python dictionary data to json
    :param data:
    :return: None
    """
    try:
        with open(DEPLOYSTATE_PATH, 'w') as json_file:
            json.dump(data, json_file)
    except Exception as e:
        print(f"Failed to store deployment state with the error: {e}\nState could not be persisted for restart.")

# Initialize the WorkspaceClient with service principal credentials
CONFIG = read_json_file(CONFIG_PATH)
WS_CONFIG = CONFIG['workspace']


WS_CLIENT = WorkspaceClient(
    host=WS_CONFIG['url'],
    client_id=WS_CONFIG['sp_client_id'],
    client_secret=WS_CONFIG['sp_client_secret']
)

dbutils = WS_CLIENT.dbutils
workspace_id = WS_CLIENT.get_workspace_id()
print("Your Workspace ID is: ", workspace_id)

def create_secret_scope() -> None:
    # creating secret scope for azure storage account
    deploy_state = read_json_file(DEPLOYSTATE_PATH)
    check_scope_name =  deploy_state.get('secret_scope_name')
    if not check_scope_name:
        secret_config = CONFIG['secrets']
        scope_name = secret_config['scope_name']

        try:
            WS_CLIENT.secrets.create_scope(
                scope=scope_name,
                initial_manage_principal="users"  # This allows all users to manage the scope
            )
            print(f"Secret scope {scope_name} created successfully.")
        except Exception as e:
            print(f"Error creating secret scope: {e}")

        #writing deployment state
        deploy_state['secret_scope_name'] = scope_name
        write_deploy_state(deploy_state)
    else:
        print("Secret scope already created. Scope Name: ", check_scope_name)


def put_secret() -> None:
    # Add a secret to the scope
    deploy_state = read_json_file(DEPLOYSTATE_PATH)
    check_secret_key = deploy_state.get('secret_key')
    if not check_secret_key:
        secret_config = CONFIG['secrets']
        scope_name = secret_config['scope_name']
        secret_key = secret_config['secret_key']
        secret_value = secret_config['secret_value']


        try:
            WS_CLIENT.secrets.put_secret(
                scope=scope_name,
                key=secret_key,
                string_value=secret_value
            )
            print(f"Secret '{secret_key}' added to scope '{scope_name}' successfully.")
        except Exception as e:
            print(f"Error adding secret: {e}")

        #writing deployment state
        deploy_state['secret_key'] = secret_key
        write_deploy_state(deploy_state)
    else:
        print("Secret key already created. Secret Key: ", check_secret_key)


def create_compute() -> None:
    # creating compute
    deploy_state = read_json_file(DEPLOYSTATE_PATH)
    check_cluster_id = deploy_state.get('cluster_id')
    scope_name = deploy_state.get('secret_scope_name')
    secret_key = deploy_state.get('secret_key')
    if not check_cluster_id:
        compute_config = CONFIG['compute']
        # Define custom Spark configurations
        custom_spark_config = {
            "spark.databricks.sql.initial.catalog.name": "hive_metastore",
            f"fs.azure.account.key.{compute_config['conf_storage_account_name']}.dfs.core.windows.net": f"{{{{secrets/{scope_name}/{secret_key}}}}}",
            "spark.databricks.delta.preview.enabled": "true"
        }

        # Create the cluster with custom configurations
        cluster_name = f"{CONFIG['name']}'s Cluster for Bootcamp"
        print("Creating Cluster: ", cluster_name)
        try:
            cluster = WS_CLIENT.clusters.create_and_wait(
                cluster_name=cluster_name,
                spark_version=CONFIG['spark_version'],
                node_type_id=compute_config['node_type_id'],
                autoscale=AutoScale(min_workers=2, max_workers=4),
                spark_conf=custom_spark_config,
                autotermination_minutes=120,
                enable_elastic_disk=True,
                data_security_mode=DataSecurityMode("USER_ISOLATION"),
                runtime_engine=RuntimeEngine("STANDARD")
            )
            print("Cluster created successfully. Cluster ID: ", cluster.cluster_id)
        except Exception as e:
            print(f"Error creating cluster: {e}")
            raise DatabricksException
        #writing deployment state
        deploy_state['cluster_id'] = cluster.cluster_id
        write_deploy_state(deploy_state)
    else:
        print("Cluster already created. Cluster ID: ", check_cluster_id)


def create_warehouse() -> None:
    # creating serverless sql warehouse
    deploy_state = read_json_file(DEPLOYSTATE_PATH)
    check_warehouse_id = deploy_state.get('warehouse_id')
    if not check_warehouse_id:
        warehouse_name = "Serverless Warehouse for Bootcamp"
        print("Creating Warehouse: ", warehouse_name)

        try:
            warehouse = WS_CLIENT.warehouses.create_and_wait(name=warehouse_name,
                                                 warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                                                 enable_serverless_compute=True,
                                                 cluster_size="2X-Small",
                                                 max_num_clusters=1,
                                                 auto_stop_mins=20,
                                                 spot_instance_policy=SpotInstancePolicy("COST_OPTIMIZED"),
                                                 channel=Channel(name=ChannelName("CHANNEL_NAME_CURRENT"))
                                                 )
            print("Warehouse created successfully. Warehouse ID: ", warehouse.id)
            warehouse_id = warehouse.id
        except Exception as e:
            print(f"Error creating warehouse: {e}")
            warehouse_id = None

        deploy_state['warehouse_id'] = warehouse_id
        write_deploy_state(deploy_state)
    else:
        print("Warehouse already created. Warehouse ID: ", check_warehouse_id)


def create_hive_tables() -> None:
    """Creates managed and external tables on Hive Metastore"""
    deploy_state = read_json_file(DEPLOYSTATE_PATH)
    check_hive_tables = deploy_state.get('hive_tables')
    if check_hive_tables != "table":
        table_config = CONFIG['tables']
        external_path_uri = table_config['external_path_uri']
        cluster_id = deploy_state.get('cluster_id')
        if not cluster_id:
            print("There was an error while retrieving cluster information from deployment state. Hive Tables creation failed.")
            raise DatabricksException

        try:
            from sql_commands import get_table_ddl_commands
        except ImportError as e:
            raise DatabricksException("Failed to import Table ddl commands.") from e
        print("Creating Hive Tables...")
        try:
            # creating execution context
            context = WS_CLIENT.command_execution.create_and_wait(
                cluster_id=cluster_id,
                language=Language.SQL
            )

            context_id = context.id
            print("Execution context created: ", context_id)
            #executing table DDLs
            execution_result = WS_CLIENT.command_execution.execute_and_wait(
                cluster_id=cluster_id,
                context_id=context_id,
                language=Language.SQL,
                command=get_table_ddl_commands(external_path_uri)
            )

            status = execution_result.status
            print("Hive Tables creation status: ", status)
            print(execution_result.results.result_type,"\nAdditional Summary: ", execution_result.results.summary)

            #Deleeting the execution context
            WS_CLIENT.command_execution.destroy(cluster_id=cluster_id, context_id=context_id)

        except Exception as e:
           raise DatabricksException("Failed to create Hive Tables.") from e


        deploy_state['hive_tables'] = execution_result.results.result_type.value
        write_deploy_state(deploy_state)
    else:
        print("Hive tables already created. Check your Hive Metastore.")


def create_dlt_hive_tables() -> None:
    """Creates and runs DLT pipelines to create MV,Streaming tables on Hive Metastore."""
    deploy_state = read_json_file(DEPLOYSTATE_PATH)
    check_dlt_tables = deploy_state.get('dlt_tables')
    if not check_dlt_tables:
        scope_name = deploy_state['secret_scope_name']
        secret_key = deploy_state['secret_key']
        spn_app_id = WS_CONFIG['sp_client_id']

        table_config = CONFIG['tables']
        external_path_uri = table_config['external_path_uri']
        azure_storage_account_name = CONFIG['compute']['conf_storage_account_name']

        ws_notebook_folder = f"/Users/{spn_app_id}/ucx-bootcamp"
        #ws_notebook_folder = "/Shared/ucx-bootcamp"
        notebook_filenames = [f for f in os.listdir('./notebooks') if 'dlt' in f]
        print("Creating Materialized Views, Streaming Tables on Hive Metastore...")
        try:
            WS_CLIENT.workspace.mkdirs(ws_notebook_folder)
            ws_notebook_path = []
            for _file in notebook_filenames:
                ws_file_path = os.path.join(ws_notebook_folder, _file)
                with open(os.path.join('./notebooks', _file), 'rb') as _f:
                    WS_CLIENT.workspace.upload(
                        path=ws_file_path,
                        content=_f,
                        format=ImportFormat("AUTO"),
                        overwrite=True
                    )

                ws_notebook_path.append(ws_file_path)

            streaming_notebook, batch_notebook = ws_notebook_path if "stream" in ws_notebook_path[0] else ws_notebook_path[::-1]
            spark_conf = {f"fs.azure.account.key.{azure_storage_account_name}.dfs.core.windows.net": f"{{{{secrets/{scope_name}/{secret_key}}}}}"}
            print("Starting a batch pipeline...")


            batch_pipeline = WS_CLIENT.pipelines.create(
                name= "ucx-bootcamp-dlt-live-external-v0",
                clusters= [PipelineCluster(
                    label= "default",
                    autoscale= PipelineClusterAutoscale(
                        min_workers= 1,
                        max_workers= 2,
                        mode= PipelineClusterAutoscaleMode("ENHANCED"),
                    ),
                    spark_conf= spark_conf
                )],
                development= True,
                continuous= False,
                channel= "CURRENT",
                photon= False,
                libraries= [
                    PipelineLibrary(notebook=NotebookLibrary(path=batch_notebook[:-4]))
                ],
                edition="ADVANCED",
                storage= str(os.path.join(external_path_uri, 'dlt_tables/batch_external')),
                target= "dlt_batch_external"
            )

            batch_pipeline_id = batch_pipeline.pipeline_id

            stream_pipeline = WS_CLIENT.pipelines.create(
                name= "ucx-bootcamp-dlt-stream-managed-v0",
                clusters= [PipelineCluster(
                    label= "default",
                    autoscale= PipelineClusterAutoscale(
                        min_workers= 1,
                        max_workers= 2,
                        mode= PipelineClusterAutoscaleMode("ENHANCED"),
                    )
                )],
                development= True,
                continuous= False,
                channel= "CURRENT",
                photon= False,
                libraries= [
                    PipelineLibrary(notebook=NotebookLibrary(path=streaming_notebook[:-4]))
                ],
                edition="ADVANCED",
                target= "dlt_stream_managed"
            )

            stream_pipeline_id = stream_pipeline.pipeline_id

            start_batch = WS_CLIENT.pipelines.start_update(batch_pipeline_id)
            start_stream = WS_CLIENT.pipelines.start_update(stream_pipeline_id)

            batch_update_id = start_batch.update_id
            stream_update_id = start_stream.update_id




            print(f'''Materialized Views, Streaming Tables are being created. Check the running pipelines for updates.
            Batch Pipeline: {WS_CONFIG['url'].rstrip('/')}/pipelines/{batch_pipeline_id}/updates/{batch_update_id}
            Stream Pipeline: {WS_CONFIG['url'].rstrip('/')}/pipelines/{stream_pipeline_id}/updates/{stream_update_id}''')
            deploy_state['dlt_tables'] = "created"
        except Exception as e:
            print(f"Error creating Materialized Views, Streaming Tables on Hive Metastore: {e}\nSkipping this step.")
            deploy_state['dlt_tables'] = "attempted"

        write_deploy_state(deploy_state)


    else:
        print("Materialized Views, Streaming Tables are already created. Check your Hive Metastore.")


def create_workspace_groups() -> None:
    """Creates workspace-level local groups."""
    deploy_state = read_json_file(DEPLOYSTATE_PATH)
    check_groups = deploy_state.get('groups')
    if not check_groups:
        group_names = CONFIG['groups']
        username = CONFIG['username']
        users = WS_CLIENT.users.list(attributes="id,UserName")
        user_id = None
        for user in users:
            if user.user_name == username:
                user_id = user.id
            else:
                user_id = user.id

        try:
            for group_name in group_names:
                group = WS_CLIENT.groups.create(display_name=group_name)
                print(f"Group: {group_name} created. Group ID: {group.id}")
                try:
                    WS_CLIENT.groups.patch(
                        id=group.id,
                        operations=[iam.Patch(
                            op=iam.PatchOp.ADD,
                            value={"members": [{
                                "value": user_id,
                            }]}
                        )],
                        schemas= [iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP]
                    )

                    print(f"User {user_id} added to the group {group_name}")

                except Exception as e:
                    print(f"Error adding user {user_id} to group {group_name}: {e}. Skipping.")

            deploy_state["groups"] = "created"

        except Exception as e:
            print(f"Error creating workspace groups: {e}\nSkipping this step.")
            deploy_state["groups"] = "attempted"

        write_deploy_state(deploy_state)

    else:
        print("Workspace local groups already created.")


def assign_grants() -> None:
    """Assigns table grants to workspace-level groups"""
    deploy_state = read_json_file(DEPLOYSTATE_PATH)
    check_grants = deploy_state.get('grants')

    if not check_grants:
        #group_names = CONFIG['groups']
        group_names = ["520738662870201", "111902696838668", "531157630628106"]
        from sql_commands import get_table_names
        resources = get_table_names()
        catalog_name = "hive_metastore"
        schemas = []
        tables = []
        for k, v in resources.items():
            _schema = f"{catalog_name}.{k}"
            schemas.append(_schema)
            _tables = [f"{_schema}.{table}" for table in v]
            tables.append(_tables)

        grant_resources = zip(group_names, schemas, tables)

        def grant_permissions(securable_type, resource_name, principal_name, privileges):
            try:
                WS_CLIENT.grants.update(
                    full_name=resource_name,
                    securable_type=securable_type,
                    changes=[
                        catalog.PermissionsChange(add=privileges, principal=principal_name)
                    ]
                )
                print(f"Successfully granted {privileges} to {principal_name} on {resource_name}")
            except Exception as e:
                print(f"Error granting permissions: {str(e)}")

        for group_name, schema, table in grant_resources:
            # grant permission on Catalog
            catalog_privileges = [
                catalog.Privilege.USE_CATALOG,
                catalog.Privilege.USAGE,
                catalog.Privilege.SELECT,
                catalog.Privilege.CREATE]

            grant_permissions(securable_type=catalog.SecurableType.CATALOG,
                              resource_name=catalog_name,
                              principal_name=group_name,
                              privileges=catalog_privileges)

            # grant permission on Schema
            schema_privileges = [
                catalog.Privilege.USE_SCHEMA,
                catalog.Privilege.USAGE,
                catalog.Privilege.SELECT,
                catalog.Privilege.CREATE,
                catalog.Privilege.MODIFY
            ]

            grant_permissions(securable_type=catalog.SecurableType.SCHEMA,
                              resource_name=schema,
                              principal_name=group_name,
                              privileges=schema_privileges)

            # grant permission on Tables
            table_privileges = [
                catalog.Privilege.SELECT,
                catalog.Privilege.MODIFY,
                catalog.Privilege.READ_FILES
            ]
            for _table in table:
                grant_permissions(securable_type=catalog.SecurableType.TABLE,
                                  resource_name=_table,
                                  principal_name=group_name,
                                  privileges=table_privileges)

        #deploy_state["grants"] = "created"
        write_deploy_state(deploy_state)

    else:
        print("Catalog, Schema & Table grants are already applied to workspace-level groups.")



print("=*=" * 50)
print("Workspace resource Deployment starting...")
print("=*=" * 50)
create_secret_scope()
print("=*=" * 50)
put_secret()
print("=*=" * 50)
create_compute()
print("=*=" * 50)
create_warehouse()
print("=*=" * 50)
create_hive_tables()
print("=*=" * 50)
create_dlt_hive_tables()
print("=*=" * 50)
create_workspace_groups()
print("=*=" * 50)
assign_grants()
print("=*=" * 50)





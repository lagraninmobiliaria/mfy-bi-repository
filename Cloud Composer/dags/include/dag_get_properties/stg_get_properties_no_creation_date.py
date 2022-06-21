import os

from pendulum import datetime
from dependencies.keys_and_constants import PROJECT_ID, STG_DATASET_MUDATA_RAW

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor

from google.cloud.bigquery import WriteDisposition, CreateDisposition, SchemaUpdateOption

source_kinds = [
    'stock', 'manual', 
    'external_feed', 'alliance'
]

with DAG(
    dag_id= 'stg_get_properties_no_creation_date',
    schedule_interval= None,
    start_date= datetime(2021, 8, 31),
    is_paused_upon_creation= True,
    params= {
        'mudata_raw': STG_DATASET_MUDATA_RAW,
        'project_id': PROJECT_ID
    }

) as dag:

    task_start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    table_id= 'properties'

    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_table_exists", 
        project_id= "{{ params.project_id }}", 
        dataset_id= "{{ params.mudata_raw }}",
        table_id= table_id,
        poke_interval= 5,
        timeout= 10
    )

    task_create_table = BigQueryCreateEmptyTableOperator(
        task_id= "create_table",
        trigger_rule= TriggerRule.ONE_FAILED,
        project_id= "{{ params.project_id }}", 
        dataset_id= "{{ params.mudata_raw }}",
        table_id= table_id,
        exists_ok= True
    )

    task_delete_properties_without_created_at = BigQueryInsertJobOperator(
        task_id= "delete_properties_without_created_at",
        trigger_rule= TriggerRule.ONE_SUCCESS,
        configuration= {
            "query": {
                "query": f"{'{%'} include './queries/delete_properties_without_created_at.sql' {'%}'}",
                "useLegacySql": False,
                "jobReference": {
                    "projectId": "{{ params.project_id }}"
                }
            }
        }
    )

    tasks= []

    get_properties_by_source_kind_query_path = os.path.join(
        os.path.dirname(__file__),
        'queries',
        'properties_by_source_kind.sql'
    )

    for source_kind in source_kinds:

        with open(get_properties_by_source_kind_query_path, 'r') as sql:
            get_properties_by_source_kind_query = sql.read().format(source_kind)
    
        task = BigQueryInsertJobOperator(
            task_id= f'get_{source_kind}_properties',
            configuration= {
                "query": {
                    "query": get_properties_by_source_kind_query,
                    "useLegacySql": False,
                    "jobReference": {
                        "projectId": "{{ params.project_id }}",
                    },
                    "destinationTable": {
                        "projectId": "{{ params.project_id }}",
                        "datasetId": "{{ params.mudata_raw }}",
                        "tableId": table_id
                    },
                    "writeDisposition": WriteDisposition.WRITE_APPEND,
                    "createDisposition": CreateDisposition.CREATE_NEVER,
                    "schemaUpdateOptions": [SchemaUpdateOption.ALLOW_FIELD_ADDITION]
                }
            }
        )


        tasks.append(task)
        

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> check_table_exists >> [task_create_table >> task_delete_properties_without_created_at]
    [task_create_table >> task_delete_properties_without_created_at] >> tasks >> task_end_dag
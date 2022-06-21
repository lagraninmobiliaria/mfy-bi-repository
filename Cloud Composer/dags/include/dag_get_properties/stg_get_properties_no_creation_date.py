import os

from pendulum import datetime
from dependencies.keys_and_constants import PROJECT_ID, STG_DATASET_MUDATA_RAW

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator, BigQueryDeleteTableOperator

from google.cloud.bigquery import QueryJobConfig, WriteDisposition, CreateDisposition, QueryJob, SchemaUpdateOption

source_kinds = [
    'stock', 'manual', 
    'external_feed', 'alliance'
]

with DAG(
    dag_id= 'stg_get_properties_no_creation_date',
    schedule_interval= None,
    start_date= datetime(2021, 8, 31),
    is_paused_upon_creation= True,
) as dag:

    task_start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    table_id= 'properties'

    task_create_table = BigQueryCreateEmptyTableOperator(
        task_id= "create_table",
        project_id= PROJECT_ID, 
        dataset_id= STG_DATASET_MUDATA_RAW,
        table_id= table_id,
    )

    tasks= []
    query_path = os.path.join(os.path.dirname(__file__),'queries','properties_by_source_kind.sql')

    for source_kind in source_kinds:
        with open(query_path, 'r') as sql:
            query = sql.read().format(source_kind)
    
        task = BigQueryInsertJobOperator(
            task_id= f'get_{source_kind}_properties',
            configuration= {
                "query": {
                    "query": query,
                    "useLegacySql": False,
                    "jobReference": {
                        "projectId": PROJECT_ID,
                    }
                }
            }
        )


        tasks.append(task)
        

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> tasks >> task_end_dag
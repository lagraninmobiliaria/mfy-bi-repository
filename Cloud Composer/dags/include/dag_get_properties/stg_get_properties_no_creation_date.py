from pendulum import datetime
from dependencies.keys_and_constants import PROJECT_ID, STG_DATASET_MUDATA_RAW

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.cloud.bigquery import QueryJobConfig, WriteDisposition, CreateDisposition, QueryJob

source_kinds = [
    'stock', 'manual', 
    'external_feed', 'alliance'
]

with DAG(
    dag_id= 'stg_get_properties_no_creation_date',
    schedule_interval= None,
    start_date= datetime(2021, 8, 31)
) as dag:

    task_start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    table_id= 'properties'
    tasks= []
    for source_kind in source_kinds:
        with open('./queries/properties_by_source_kind.sql') as sql:
            query = sql.read().format(source_kind)
    
        task = BigQueryInsertJobOperator(
            task_id= f'get_{source_kind}_properties',
            configuration= {
                "query": {
                    "query": query,
                    "useLegacySql": False,
                    "jobReference": {
                        "projectId": PROJECT_ID,
                    },
                    # "destinationTable": {
                    #     "projectId": PROJECT_ID,
                    #     "datasetId": STG_DATASET_MUDATA_RAW,
                    #     "tableId": table_id
                    # }, 
                    # "writeDisposition": WriteDisposition.WRITE_APPEND,
                    # "createDisposition": CreateDisposition.CREATE_NEVER,
                }
            }
        )


        tasks.append(task)
        

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> tasks >> task_end_dag
from datetime import datetime

from dependencies.keys_and_constants import STG_PARAMS

from include.dag_get_tickets_creation.functions import DAGQueriesManager

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.cloud.bigquery import WriteDisposition, CreateDisposition

with DAG(
    dag_id= "stg_get_tickets_creation",
    schedule_interval= "@daily",
    start_date= datetime(2020,4,13),
    end_date= datetime(2020, 5, 1),
    tags= ['staging'],
    params= STG_PARAMS,
    is_paused_upon_creation= True
) as dag:

    task_start_dag= DummyOperator(
        task_id= 'start_dag'
    )
    
    queries_manager= DAGQueriesManager()
    get_tickets_creation_query= queries_manager.get_tickets_creation_query_template

    task_get_tickets_creation= BigQueryInsertJobOperator(
        task_id= "get_tickets_creation",
        configuration= {
            "query": {
                "query": get_tickets_creation_query,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": dag.params['project_id'],
                    "datasetId": dag.params['mudata_raw'],
                    "tableId": 'smith_tickets_creation'
                },
                "writeDisposition": WriteDisposition.WRITE_APPEND,
                "createDisposition": CreateDisposition.CREATE_NEVER
            }
        }
    )

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_get_tickets_creation >> task_end_dag
from datetime import datetime

import py

from dependencies.keys_and_constants import DATASET_MUDATA_RAW, PROJECT_ID, createDisposition, writeDisposition

from include.dag_get_first_questionevent import queries 
from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import PythonOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

with DAG(
    dag_id= 'get_first_questionevent',
    schedule_interval= '*/30 * * * *',
    start_date= datetime(2020, 4, 8),
    end_date= datetime(2020, 4, 9),
    catchup= True
) as dag:

    datetime_floor = "{{data_interval_start}}"
    datetime_ceil  = "{{data_interval_end}}"
    
    start_dag = PythonOperator(
        task_id= 'start_dag',
        python_callable= lambda x: print(
            datetime_floor,
            type(datetime_floor),
            datetime_ceil,
            type(datetime_ceil),
            sep= '\n'
        )
    )


    query = queries.get_client_first_questionevent(datetime_floor, datetime_ceil)
    table_id = 'first_questionevent' 

    query_new_questionevents = BigQueryInsertJobOperator(
        gcp_conn_id= "bigquery_default",
        location= 'us',
        task_id= 'query_new_questionevents',
        configuration= {
            "query": {
                "query": query,
                "useLegacySql": False,
                "jobReference": {
                    "projectId": PROJECT_ID,
                },
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_MUDATA_RAW,
                    "tableId": table_id
                }, 
                "writeDisposition": writeDisposition.WRITE_APPEND,
                "createDisposition": createDisposition.CREATE_IF_NEEDED,
            }
        },
    )

    end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    start_dag >> query_new_questionevents >> end_dag
from datetime import datetime

from dependencies.keys_and_constants import DATASET_MUDATA_RAW, PROJECT_ID, writeDisposition, createDisposition

from airflow                                            import DAG
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

with DAG(
    dag_id= "get_listed_property_events",
    schedule_interval= "@daily",
    start_date= datetime(2020, 5, 16),
    is_paused_upon_creation= True,
    user_defined_macros= {
        "polymorphic_ctype_id": 77
    }
) as dag:
    
    task_start_dag = DummyOperator(
        task_id=  'start_dag'
    )

    SQL_QUERY_PATH= f'./queries/{dag.dag_id}.sql'
    table_id= 'listed_property_events'

    task_get_listed_property_events = BigQueryInsertJobOperator(
        task_id= 'get_listed_property_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
                "jobReference": {
                    "projectId": PROJECT_ID
                },
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_MUDATA_RAW,
                    "tableId": table_id
                },
                "writeDisposition": writeDisposition.WRITE_APPEND,
                "createDisposition": createDisposition.CREATE_IF_NEEDED
            }
        }
    ) 

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_get_listed_property_events >> task_end_dag
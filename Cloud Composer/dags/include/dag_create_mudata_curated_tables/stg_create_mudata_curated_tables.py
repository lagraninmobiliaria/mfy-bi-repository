from datetime import datetime

from include.dag_create_mudata_curated_tables.table_schemas import *
from dependencies.keys_and_constants import PROJECT_ID, STG_DATASET_MUDATA_CURATED

from airflow                                            import DAG
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryCreateEmptyTableOperator

from google.cloud import bigquery

bigquery.SchemaField.name

tables_to_create = [
    LOOK_PROPERTIES, LOOK_PROPERTIES_ADDITIONAL, FACT_PROPERTIES, 
    LOOK_CLIENTS, FACT_CLIENTS,
    LOOK_DEVELOPMENTS
]

with DAG(
    dag_id= "stg_create_mudata_curated_tables",
    schedule_interval= None,
    start_date= datetime(2021, 1, 1),
    is_paused_upon_creation= True,
    tags= ['staging'],
    params= {
        'project_id': PROJECT_ID,
        'mudata_raw': STG_DATASET_MUDATA_CURATED
    }
) as dag:
    
    task_start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    tasks_table_creation = []

    for table_to_create in tables_to_create:
        task_create_table = BigQueryCreateEmptyTableOperator(
            task_id= f"create_table_{table_to_create.get('table_id')}",
            project_id= "{{ params.project_id }}",
            dataset_id= "{{ params.mudata_raw }}",
            table_id= table_to_create.get("table_id"),
            schema_fields= table_to_create.get("schema_fields"),
            time_partitioning= table_to_create.get("time_partitioning"),
            cluster_fields= table_to_create.get("cluster_fields"),
            exists_ok= True
        )

        tasks_table_creation.append(task_create_table)

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> tasks_table_creation >> task_end_dag
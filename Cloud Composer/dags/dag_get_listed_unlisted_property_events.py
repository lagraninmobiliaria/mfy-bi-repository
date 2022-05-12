from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator
 
from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, writeDisposition, createDisposition
from include.sql import queries

default_args = {
    'retries': 0,
}

with DAG(
    dag_id= 'get_listed_and_unlisted_propertyevents.py',
    start_date= datetime(2021, 5, 3),   
    schedule_interval= '@daily',
    default_args= default_args,
    catchup= False,
) as dag:
    # This task is a BigQueryJob that
    # fetchs the listed and unlisted propertyevents from pogresql and 
    # it stores them in the raw dataset 
    date = "{{ ds }}"
    table_id= "listed_and_unlisted_propertyevents"
    sql = queries.listed_and_unlisted_propertyevents(date= date)
    
    bq_job_get_events = BigQueryInsertJobOperator(
        gcp_conn_id= "bigquery_default",
        task_id= 'get_listed_unlisted_propertyevents',
        configuration= {
            "query": {
                "query": sql,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_MUDATA_RAW,
                    "tableId": table_id
                }, 
                "writeDisposition": writeDisposition.WRITE_APPEND,
                "createDisposition": createDisposition.CREATE_IF_NEEDED,
            }
        },
        b
    )

    # This task fetchs the listed and unlisted from Google Cloud Storage and 
    # it stores them in Google BigQuery
    
    # from_gcs_to_bquery = GCSToBigQueryOperator(
    #     task_id='from_gcs_to_bquery',
    #     bucket= EXTERNAL_DATA_BUCKET,
    #     source_objects= '',
    #     destination_project_dataset_table=f"{DATASET_MUDATA_RAW}.listed_and_unlisted_property_events",
    #     # schema_fields=[
    #     #     {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     #     {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
    #     # ],
    #     create_disposition= 'CREATE_IF_NEEDED',  
    #     write_disposition='WRITE_APPEND',
    # )


    bq_job_get_events
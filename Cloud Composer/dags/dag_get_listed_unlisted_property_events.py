from fileinput import filename
from xml.etree.ElementInclude import include
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
# from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
 
from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, writeDisposition, createDisposition
from include.sql import queries

with DAG(
    dag_id= 'get_listed_and_unlisted_property_events.py',
    start_date= datetime(2021, 5, 3),   
    schedule_interval= '@daily',
    catchup= False,    
) as dag:
    # This task is a BigQueryJob that
    # fetchs the listed and unlisted propertyevents from pogresql and 
    # it stores them in the raw dataset 
    date = "{{ ds }}"
    sql = queries.listed_and_unlisted_property_events(date= date)
    bq_job_get_events = BigQueryExecuteQueryOperator(
        task_id= 'get_listed_unlisted_propertyevents',
        sql= sql,
        destination_dataset_table= f"{PROJECT_ID}.{DATASET_MUDATA_RAW}.listed_unlisted_propertyevents",
        write_disposition= writeDisposition.WRITE_APPEND,
        create_disposition= createDisposition.CREATE_IF_NEEDED,
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
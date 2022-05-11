from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
 
from datetime import datetime

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_RAW, EXTERNAL_DATA_BUCKET
from include.sql import queries

with DAG(
    dag_id= 'get_listed_unlisted_events.py',
    start_date= datetime(2021, 5, 3),
    schedule_interval= '@daily',
    catchup= True,    
) as dag:
    # This task fetchs the listed and unlisted from pogresql and 
    # it stores them in Google Cloud Storage
    from_pgsql_to_gcs = PostgresToGCSOperator(
        task_id= 'from_pgsql_to_gcs',
        postgres_conn_id= 'postgres_conn',
        sql= queries.listed_and_unlisted_events(date= "{{ ds }}"),
        bucket= EXTERNAL_DATA_BUCKET,
        gzip=False,
        use_server_side_cursor=True,
    )
    # This task fetchs the listed and unlisted from Google Cloud Storage and 
    # it stores them in Google BigQuery
    
    from_gcs_to_bquery = GCSToBigQueryOperator(
        task_id='from_gcs_to_bquery',
        bucket= EXTERNAL_DATA_BUCKET,
        destination_project_dataset_table=f"{DATASET_MUDATA_RAW}.listed_and_unlisted_events",
        # schema_fields=[
        #     {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {'name': 'post_abbr', 'type': 'STRING', 'mode': 'NULLABLE'},
        # ],
        create_disposition= 'CREATE_IF_NEEDED',  
        write_disposition='WRITE_APPEND',
    )


    pass
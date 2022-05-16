#TODO Align imports to make it look nicer

from datetime import datetime, timedelta

from dependencies.keys_and_constants import PROJECT_ID, DATASET_MUDATA_CURATED, createDisposition, writeDisposition

from include.sql import queries

import pandas as pd

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.python                           import PythonOperator
from airflow.operators.dummy                            import DummyOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

from google.cloud.bigquery import Client, LoadJobConfig

from dependencies.net_daily_propertyevents_funcs import net_daily_propertyevents

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds= 60)
}

def task_net_daily_propertyevents(ti):
    bq_client = Client(project= PROJECT_ID, location= 'US')
    
    job_id = ti.xcom_pull(task_ids= 'query_daily_propertyevents')
    bq_job = bq_client.get_job(job_id= job_id)    
    query_results = bq_job.to_dataframe()

    properties = query_results.prop_id.unique()

    data = []

    for prop in properties:
        prop_events_df = query_results[query_results.prop_id == prop].copy().reset_index(drop= True).sort_values(by= 'created_at', ascending= True)
        events_balance_result = net_daily_propertyevents(prop_df= prop_events_df)
        if events_balance_result != ():
            data.append((ti.execution_date.date(), prop) + events_balance_result)

    df_to_append = pd.DataFrame(data, columns= ['registered_date', 'prop_id', 'is_listing', 'is_unlisting'])

    bq_client.load_table_from_dataframe(
        dataframe= df_to_append,
        destination= f"{PROJECT_ID}.{DATASET_MUDATA_CURATED}.intermediate_daily_net_property_events",
        job_config= LoadJobConfig(
            create_disposition= createDisposition.CREATE_IF_NEEDED,  
            write_disposition= writeDisposition.WRITE_APPEND
        )
    )

with DAG(
    dag_id= 'net_daily_propertyevents',
    start_date= datetime(2021, 5, 3),   
    schedule_interval= '@daily',
    default_args= default_args,
    catchup= False
) as dag:

    date = "{{ ds }}"


    start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    query_daily_propertyevents = BigQueryInsertJobOperator(
        task_id= 'query_daily_propertyevents',
        configuration= {
            "query": {
                "query": queries.get_daily_propertyevents(date),
                "useLegacySql": False,
                "jobReference": {
                    "projectId": PROJECT_ID,
                }
            }
        }
    )

    py_net_daily_propertyevents = PythonOperator(
        task_id= 'net_daily_propertyevents',
        python_callable= task_net_daily_propertyevents
    )

    end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    start_dag >> query_daily_propertyevents >> py_net_daily_propertyevents >> end_dag
from datetime import datetime, timedelta
from textwrap import dedent

import py

from dependencies.keys_and_constants                    import PROJECT_ID, DATASET_MUDATA_RAW, writeDisposition, createDisposition
from include.dag_get_listed_unlisted_propertyevents     import queries

from google.cloud.bigquery import Client

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import PythonOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds= 60)
}

def log_DAG_results_description(ti):
    bq_client = Client(project= PROJECT_ID)
    job_id = ti.xcom_pull(task_ids= 'query_daily_propertyevents')
    bq_job = bq_client.get_job(job_id= job_id)    
    query_results = bq_job.to_dataframe()

    print(dedent(
        f"""
        Date:{str(ti.execution_date.date()):>10}
        Daily Events: {str(int(query_results.shape[0])):>10}
        Properties: {str(int(query_results.prop_id.nunique())):>10}
        """
    ))

with DAG(
    dag_id= 'get_listed_and_unlisted_propertyevents',
    start_date= datetime(2021, 5, 3),   
    schedule_interval= '@daily',
    default_args= default_args,
    catchup= True,
) as dag:

    start_task = DummyOperator(
        task_id= 'start_dag'
    )

    # This task is a BigQueryJob that
    # fetchs the listed and unlisted propertyevents from pogresql and 
    # it stores them in the raw dataset 
    date = "{{ ds }}"
    table_id= "listed_and_unlisted_propertyevents"
    sql = queries.listed_and_unlisted_propertyevents(date= date)

    bq_job_get_events = BigQueryInsertJobOperator(
        gcp_conn_id= "bigquery_default",
        task_id= 'get_listed_unlisted_propertyevents',
        location= 'us',
        configuration= {
            "query": {
                "query": sql,
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

    query_daily_propertyevents = BigQueryInsertJobOperator(
        task_id= 'query_daily_propertyevents',
        configuration= {
            "query": {
                "query": queries.get_daily_propertyevents(project_id= PROJECT_ID, dataset= DATASET_MUDATA_RAW, date= date),
                "useLegacySql": False,
                "jobReference": {
                    "projectId": PROJECT_ID,
                }
            }
        }
    )

    py_log_DAG_results_description = PythonOperator(
        task_id= 'log_dag_results_description',
        python_callable= log_DAG_results_description,
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    end_task = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    start_task >> bq_job_get_events >> query_daily_propertyevents >> py_log_DAG_results_description >> end_task
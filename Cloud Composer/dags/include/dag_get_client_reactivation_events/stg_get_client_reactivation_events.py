from datetime import datetime, timedelta

from dependencies.keys_and_constants import STG_DATASET_MUDATA_RAW, PROJECT_ID

from include.dag_get_client_reactivation_events.functions import validate_search_reactivation_as_client_reactivation, previous_dagrun_successful

from airflow import DAG
from airflow.sensors.external_task                          import ExternalTaskSensor
from airflow.sensors.python                                 import PythonSensor
from airflow.operators.dummy                                import DummyOperator
from airflow.operators.python                               import PythonOperator
from airflow.providers.google.cloud.operators.bigquery      import BigQueryInsertJobOperator, BigQueryCreateEmptyTableOperator


from google.cloud.bigquery import TimePartitioningType

with DAG(
    dag_id= 'stg_get_client_reactivation_events',
    schedule_interval= '@daily',
    max_active_runs= 1, 
    start_date= datetime(2021, 10, 19),
    end_date= datetime(2021, 11, 1),
    params= {
        'project_id': PROJECT_ID,
        'mudata_raw': STG_DATASET_MUDATA_RAW,
        'env_prefix': 'stg'
    },
    is_paused_upon_creation= True
) as dag:

    sensor_check_dag_closed_clients_events= ExternalTaskSensor(
        task_id= "check_dag_closed_clients_events",
        poke_interval= 30,
        timeout= 30*20,
        external_dag_id= "{{ params.env_prefix }}" + '_get_closed_client_events',
        external_task_id= "end_dag"
    )

    sensor_check_dag_search_reactivations_events= ExternalTaskSensor(
        task_id= "check_dag_search_reactivations_events",
        poke_interval= 30,
        timeout= 30*20,
        external_dag_id= "{{ params.env_prefix }}" + "_get_search_reactivation_events",
        external_task_id= "end_dag"
    )

    sensor_check_successful_previous_dagrun= PythonSensor(
        task_id= "check_successful_previous_dagrun",
        python_callable= previous_dagrun_successful
    )

    task_start_dag = DummyOperator(
        task_id= 'start_dag'
    )

    task_create_client_reactivation_table = BigQueryCreateEmptyTableOperator(
        task_id= 'create_client_reactivation_events_table',
        exists_ok= True,
        project_id= "{{ params.project_id }}",
        dataset_id= "{{ params.mudata_raw }}",
        table_id= 'client_reactivation_events',
        schema_fields= [
            {'name': 'event_id', 'type': 'INTEGER'},
            {'name': 'created_at', 'type': 'TIMESTAMP'},
            {'name': 'client_id', 'type': 'INTEGER'}
        ],
        time_partitioning= {
            "field": "created_at",
            "type": TimePartitioningType.DAY
        },
        cluster_fields= ['client_id', 'event_id']
    )

    SQL_QUERY_PATH= f'./queries/daily_search_reactivation_events.sql'

    task_query_daily_search_reactivation_events = BigQueryInsertJobOperator(
        task_id= 'query_daily_search_reactivation_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
            }
        }
    )

    task_search_reactivations_as_client_reactivations= PythonOperator(
        task_id= 'search_reactivations_as_client_reactivations',
        python_callable= validate_search_reactivation_as_client_reactivation,
    )

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    [sensor_check_dag_closed_clients_events, sensor_check_dag_search_reactivations_events, sensor_check_successful_previous_dagrun] >> task_start_dag
    task_start_dag >> task_create_client_reactivation_table
    task_create_client_reactivation_table >> task_query_daily_search_reactivation_events >> task_search_reactivations_as_client_reactivations
    task_search_reactivations_as_client_reactivations >> task_end_dag
    
    
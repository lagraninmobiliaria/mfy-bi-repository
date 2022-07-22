from __future__ import with_statement
from datetime import datetime

from dependencies.keys_and_constants import PROD_PARAMS

from include.dag_get_buying_opportunity_cases.functions import DAGQueriesManager, get_ticket_id_for_buying_opportunity_cases, previous_dagrun_successful

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG(
    dag_id= 'prod_get_buying_opportunity_cases',
    schedule_interval= "@daily",
    start_date= datetime(2020, 4, 13),
    max_active_runs= 1, 
    tags=['production'],
    params= PROD_PARAMS,
    is_paused_upon_creation= True
) as dag:
    
    sensor_check_tickets_creation_dagrun= ExternalTaskSensor(
        task_id= "check_tickets_creation_dagrun",
        external_dag_id= dag.params['env_prefix'] + '_get_tickets_creation',
        external_task_id= 'end_dag',
        poke_interval= 30,
        timeout= 60*30
    )

    sensor_check_previous_dagrun_successful= PythonSensor(
        task_id= "check_previous_dagrun_successful",
        python_callable= previous_dagrun_successful,
        poke_interval= 30,
        timeout= 15*30,
    )

    task_start_dag= DummyOperator(
        task_id= 'start_dag'
    )

    queries_manager= DAGQueriesManager()
    get_buying_opportunity_cases_query= queries_manager.get_buying_opportunity_cases_query_template

    task_get_buying_opportunity_cases= BigQueryInsertJobOperator(
        task_id= "get_buying_opportunity_cases",
        configuration= {
            "query": {
                "query": get_buying_opportunity_cases_query,
                "useLegacySql": False
            }
        }
    )

    task_get_ticket_id_and_load_buying_opp_case= PythonOperator(
        task_id= "get_ticket_id_and_load_buying_opp_case",
        python_callable= get_ticket_id_for_buying_opportunity_cases
    )

    task_end_dag= DummyOperator(
        task_id= 'end_dag'
    )

    [sensor_check_tickets_creation_dagrun, sensor_check_previous_dagrun_successful] \
    >> task_start_dag >> task_get_buying_opportunity_cases \
    >> task_get_ticket_id_and_load_buying_opp_case >> task_end_dag

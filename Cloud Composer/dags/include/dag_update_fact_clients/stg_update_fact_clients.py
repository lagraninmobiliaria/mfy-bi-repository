from datetime import datetime

from dependencies.keys_and_constants import STG_PARAMS

from include.dag_update_fact_clients.functions          import DAGQueriesManager, load_new_clients_to_fact_table, load_clients_closures_and_reactivations_to_fact_table

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import PythonOperator
from airflow.sensors.external_task                      import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

with DAG(
    dag_id= 'stg_update_fact_clients',
    start_date= datetime(2020, 4, 13),
    schedule_interval= "@daily",
    max_active_runs= 1,
    is_paused_upon_creation= True,
    catchup= True,
    tags= ['staging'],
    params= STG_PARAMS
) as dag:

    queries_manager= DAGQueriesManager()

    sensor_check_previous_dagrun_successful= ExternalTaskSensor(
        task_id= "check_previous_dagrun_successful",
        poke_interval= 30,
        timeout= 30*60,
        external_dag_id= "{{ params.env_prefix }}" + "_update_fact_clients",
        external_task_id= "end_dag"
    )

    # sensor_check_clients_creations_dagrun= ExternalTaskSensor(
    #     task_id= 'check_clients_creations',
    #     poke_interval= 30,
    #     timeout= 30*60,
    #     external_dag_id= STG_PARAMS.get('env_prefix') + "_get_clients_info_for_client_creation",
    #     external_task_id= "end_dag"
    # )

    # sensor_check_clients_reactivations_dagrun= ExternalTaskSensor(
    #     task_id= 'check_clients_reactivations',
    #     poke_interval= 30,
    #     timeout= 30*60,
    #     external_dag_id= STG_PARAMS.get('env_prefix') + "_get_client_reactivation_events",
    #     external_task_id= "end_dag"
    # )

    # sensor_check_clients_closure_dagrun= ExternalTaskSensor(
    #     task_id= 'check_clients_closure',
    #     poke_interval= 30,
    #     timeout= 30*60,
    #     external_dag_id= STG_PARAMS.get('env_prefix') + "_get_closed_client_events",
    #     external_task_id= "end_dag"
    # )

    task_start_dag= DummyOperator(
        task_id= "start_dag",
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    task_get_client_creation_events= BigQueryInsertJobOperator(
        task_id= "get_client_creation_events",
        configuration= {
            "query": {
                "query": queries_manager.clients_creation_events_query,
                "useLegacySql": False
            }
        }
    )

    task_get_client_reactivation_events= BigQueryInsertJobOperator(
        task_id= "get_client_reactivation_events",
        configuration= {
            "query": {
                "query": queries_manager.client_reactivation_events_query,
                "useLegacySql": False
            }
        }
    )

    task_get_closed_client_events= BigQueryInsertJobOperator(
        task_id= "get_closed_client_events",
        configuration= {
            "query": {
                "query": queries_manager.closed_client_events_query,
                "useLegacySql": False
            }
        }
    )

    task_load_new_clients_to_fact_table= PythonOperator(
        task_id= 'load_new_clients_to_fact_table',
        python_callable= load_new_clients_to_fact_table,
    )

    task_load_clients_closures_and_reactivations_to_fact_table= PythonOperator(
        task_id= 'load_clients_closures_and_reactivations_to_fact_table',
        python_callable= load_clients_closures_and_reactivations_to_fact_table
    )

    task_end_dag= DummyOperator(
        task_id= "end_dag"
    )

    sensor_check_previous_dagrun_successful >> task_start_dag
    # [sensor_check_clients_creations_dagrun, sensor_check_clients_reactivations_dagrun, sensor_check_clients_closure_dagrun] >> task_start_dag
    task_start_dag >> [task_get_client_creation_events, task_get_client_reactivation_events, task_get_closed_client_events] \
    >> task_load_new_clients_to_fact_table \
    >> task_load_clients_closures_and_reactivations_to_fact_table >> task_end_dag
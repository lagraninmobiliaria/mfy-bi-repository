from datetime import datetime

from dependencies.keys_and_constants import STG_PARAMS

from include.dag_update_fact_clients.functions          import DAGQueriesManager, load_new_clients_to_fact_table, load_clients_closures_and_reactivations_to_fact_table

from airflow                                            import DAG
from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import PythonOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

with DAG(
    dag_id= 'stg_update_fact_clients',
    start_date= datetime(2022, 1, 1),
    end_date= datetime(2022, 2, 1),
    schedule_interval= "@daily",
    max_active_runs= 1,
    is_paused_upon_creation= True,
    catchup= True,
    tags= ['staging'],
    params= STG_PARAMS
) as dag:

    queries_manager= DAGQueriesManager()

    task_start_dag= DummyOperator(
        task_id= "start_dag"
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

    task_start_dag >> [task_get_client_creation_events, task_get_client_reactivation_events, task_get_closed_client_events] \
    >> task_load_new_clients_to_fact_table \
    >> task_load_clients_closures_and_reactivations_to_fact_table >> task_end_dag
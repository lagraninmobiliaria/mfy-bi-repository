from datetime import datetime

from textwrap import dedent

from dependencies.keys_and_constants import DATASET_MUDATA_RAW, PROJECT_ID

from airflow                                                import DAG
from airflow.utils.trigger_rule                             import TriggerRule
from airflow.operators.dummy                                import DummyOperator
from airflow.operators.bash                                 import BashOperator
from airflow.providers.google.cloud.operators.bigquery      import BigQueryInsertJobOperator

with DAG(
    dag_id= 'get_search_reactivation_events',
    schedule_interval= '@daily',
    start_date= datetime(2021, 10, 19),
    is_paused_upon_creation= True,
    params= {
        "polymorphic_ctype_id": 94
    }
) as dag:

    task_start_dag = BashOperator(
        task_id= 'start_dag',
        bash_command= "echo DAGRun started: {{ data_interval_start }} - {{ data_interval_end }}"
    )


    SQL_QUERY_PATH= f'./queries/{dag.dag_id}.sql'
    destionation_table_id= 'search_reactivation_events'

    task_get_search_reactivation_events = BigQueryInsertJobOperator(
        task_id= 'get_search_reactivation_events',
        configuration= {
            "query": {
                "query": f"{'{%'} include '{SQL_QUERY_PATH}' {'%}'}",
                "useLegacySql": False,
                # "destionationTable": {
                #     "projectId": PROJECT_ID,
                #     "datasetId": DATASET_MUDATA_RAW,
                #     "tableId": destionation_table_id
                # }
            }
        }
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ALL_SUCCESS
    )

    task_start_dag >> task_get_search_reactivation_events >> task_end_dag

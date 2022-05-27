from datetime import datetime

from airflow import DAG
from airflow.operators.python                           import PythonOperator
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.bash                             import BashOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator

with DAG(
    dag_id= 'get_questionevents',
    schedule_interval= '@daily',
    start_date= datetime(2020, 4, 8),
    end_date= datetime(2020, 4, 9),
    is_paused_upon_creation= True,
) as dag:

    date = "{{ ds }}"

    task_start_dag = BashOperator(
        task_id = 'start_dag',
        bash_command= f"echo Start DAGRun - Logical date: {date}"
    )

    task_get_questionevents = BigQueryInsertJobOperator(
        task_id= 'get_questionevents',
        configuration= {
            "query": {
                "query": "{% include './include/dag_get_questionevents/queries/get_questionevents.sql' %}",
                "useLegancySql": False,
            }
        }
    )

    task_end_dag = DummyOperator(
        task_id= 'end_dag'
    )

    task_start_dag >> task_get_questionevents >> task_end_dag
from datetime import datetime
 
from airflow                                            import DAG

from airflow.utils.trigger_rule                         import TriggerRule
from airflow.operators.dummy                            import DummyOperator
from airflow.operators.python                           import BranchPythonOperator

def is_first_run(**context):
    prev_data_interval_start_success = context.get('prev_data_interval_start_success')
    print(prev_data_interval_start_success)
    if prev_data_interval_start_success is None:
        return 'branch_a'
    else:
        return 'branch_b'

with DAG(
    dag_id= 'get_client_first_questionevent',
    schedule_interval= '*/30 * * * *',
    start_date= datetime(2020, 4, 8),
    end_date= datetime(2020, 4, 9),
    max_active_runs= 1,
    is_paused_upon_creation= True,
    catchup= True
) as dag:
    
    start_dag = DummyOperator(
        task_id= 'start_dag',
    )
    
    branch_task = BranchPythonOperator(
        task_id= 'task_is_first_run',
        python_callable= is_first_run,
    ) 

    task_branch_a = DummyOperator(
        task_id= 'branch_a'
    )
    task_branch_b = DummyOperator(
        task_id= 'branch_b'
    )


    end_dag = DummyOperator(
        task_id= 'end_dag',
        trigger_rule= TriggerRule.ONE_SUCCESS
    )

    start_dag >> branch_task >> [task_branch_a, task_branch_b] >> end_dag
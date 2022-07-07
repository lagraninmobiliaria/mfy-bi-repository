import os

from google.cloud.bigquery import Client

from pandas import DataFrame
from numpy import nan

from datetime import datetime

class DAGQueriesManager:

    def __init__(
        self, 
        clients_creation_events_file= 'get_client_creation_events.sql',
        client_reactivation_events_file= 'get_client_reactivation_events.sql',
        closed_client_events_file= 'get_closed_client_events.sql',
    ) -> None:
        
        clients_creation_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            clients_creation_events_file
        )
        with open(clients_creation_query_path, 'r') as sql_file:
            self.clients_creation_events_query= sql_file.read()

        client_reactivation_events_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            client_reactivation_events_file
        )
        with open(client_reactivation_events_query_path, 'r') as sql_file:
            self.client_reactivation_events_query= sql_file.read()

        closed_client_events_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            closed_client_events_file
        )
        with open(closed_client_events_query_path, 'r') as sql_file:
            self.closed_client_events_query= sql_file.read()

def update_fact_clients_table(**context):
    
    bq_client= Client(project= context['params'].get('project_id'), location= 'us-central1')
    
    bq_job_id_get_client_creation_events= context['task_instance'].xcom_pull('get_client_creation_events')
    bq_job_get_client_creation_events= bq_client.get_job(job_id= bq_job_id_get_client_creation_events)
    df_creation_events= bq_job_get_client_creation_events.to_dataframe()

    rows_for_new_clients(df_creation_events= df_creation_events, bq_client= bq_client, **context)
    
    bq_job_id_get_client_reactivation_events= context['task_instance'].xcom_pull('get_client_reactivation_events')
    bq_job_get_client_reactivation_events= bq_client.get_job(job_id= bq_job_id_get_client_reactivation_events)
    df_reactivation_events= bq_job_get_client_reactivation_events.to_dataframe()
    
    bq_job_id_get_closed_client_events= context['task_instance'].xcom_pull('get_closed_client_events')
    bq_job_get_closed_client_events= bq_client.get_job(job_id= bq_job_id_get_closed_client_events)
    df_closed_client_events= bq_job_get_closed_client_events.to_dataframe()

def rows_for_new_clients(df_creation_events: DataFrame, bq_client: Client, **context) -> list:
    
    rows= []

    for index in range(df_creation_events.shape[0]):
        row= dict(
            client_id= df_creation_events.iloc[index].client_id,
            from_datetime_z= df_creation_events.iloc[index].creation_datetime_z,
            to_datetime_z= nan,
            is_active= True,
            is_reactive=False,
            last_modified_datetime_z= context['data_interval_start'],
        )

        print(row)
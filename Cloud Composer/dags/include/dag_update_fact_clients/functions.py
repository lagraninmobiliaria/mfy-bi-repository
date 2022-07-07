from genericpath import exists
import os
from posixpath import dirname

from google.cloud.bigquery import Client
from grpc import Status

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

    creation_clients_rows= rows_for_new_clients(df_creation_events= df_creation_events, bq_client= bq_client, **context)
    print(creation_clients_rows)
    
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
            from_datetime_z= df_creation_events.iloc[index].creation_datetime_z
                .to_pydatetime(),
            to_datetime_z= nan,
            is_active= True,
            is_reactive=False,
            last_modified_datetime_z= context['data_interval_start'],
        )

        check_record_existance_query_path= os.path.join(
            dirname(__file__),
            'queries',
            'client_creation_already_exists.sql'
        )
        with open(check_record_existance_query_path, 'r') as sql_file:
            check_record_existance_query= sql_file.read().format(
                project_id= context['params'].get('project_id'),
                dataset_id= context['params'].get('mudata_curated'),
                table_id= 'fact_clients',
                field_name= 'client_id',
                field_value= row.get('client_id')
            )

        record_exists= bq_client\
            .query(query= check_record_existance_query)\
            .result()\
            .total_rows >= 1

        if not record_exists:
            rows.append(row)
    
    return rows
    

    
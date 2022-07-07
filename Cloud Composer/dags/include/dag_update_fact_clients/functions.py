import os

from datetime import datetime
from webbrowser import get

from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, CreateDisposition

from pandas import DataFrame
from numpy import nan

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

def load_new_clients_to_fact_table(**context):
    
    bq_client= Client(project= context['params'].get('project_id'), location= 'us-central1')
    
    bq_job_id= context['task_instance'].xcom_pull('get_client_creation_events')
    bq_job= bq_client.get_job(job_id= bq_job_id)
    df_creation_events= bq_job.to_dataframe()

    df_new_clients= get_df_new_clients(df_creation_events= df_creation_events, bq_client= bq_client, **context)
    
    load_job= bq_client.load_table_from_dataframe(
        dataframe= df_new_clients,
        destination= f"{context['params'].get('project_id')}.{context['params'].get('mudata_curated')}.fact_clients", 
        job_config= LoadJobConfig(
            create_disposition= CreateDisposition.CREATE_NEVER,
            write_disposition= WriteDisposition.WRITE_APPEND
        )
    )

    return load_job.job_id

def get_df_new_clients(df_creation_events: DataFrame, bq_client: Client, **context) -> DataFrame:
    
    client_that_already_exists_query_path= os.path.join(
        os.path.dirname(__file__),
        'queries',
        'client_that_already_exists.sql'
    )
    with open(client_that_already_exists_query_path) as sql_file:
        client_that_already_exists_query= sql_file.read().format(
            project_id= context['params'].get("project_id"),
            dataset_id= context['params'].get("mudata_curated"),
            list_client_ids= tuple(df_creation_events.client_id.unique())
        )
    
    client_that_already_exists_results= bq_client.query(
        query= client_that_already_exists_query
    ).to_dataframe().client_id.unique()

    mask_clients_dont_exist= ~df_creation_events.client_id.isin(client_that_already_exists_results)

    df_creation_events_to_append= df_creation_events[mask_clients_dont_exist]\
        .copy() \
        .reset_index(drop= True)\
        [['client_id', 'creation_datetime_z']]
    
    df_creation_events_to_append['from_datetime_z'] = df_creation_events_to_append['creation_datetime_z'].apply(lambda x: x.to_pydatetime())
    df_creation_events_to_append.drop(columns=['creation_datetime_z'], inplace= True)
    df_creation_events_to_append['to_datetime_z'] = nan
    df_creation_events_to_append['is_active']= True
    df_creation_events_to_append['is_reactive']= False
    df_creation_events_to_append['last_modified_datetime_z']= context['data_interval_start']

    return df_creation_events_to_append
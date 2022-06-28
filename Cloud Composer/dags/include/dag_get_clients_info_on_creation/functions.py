import os
from textwrap import dedent
from google.cloud.bigquery import Client
from pandas import DataFrame


def client_exists_already(bq_client: Client, client_id, **context):
    
    query= dedent(f"""
        SELECT
            client_id
        FROM `{context['params'].get('project_id')}.{context['params'].get('mudata_raw')}.clients_creation` clients_creation
        WHERE 
        clients_creation.client_id = {client_id};
    """)
    
    bq_job= bq_client.query(
        query= query
    )

    exists = bq_job.result().total_rows >= 1

    return exists

def new_clients_to_load(df_clients_with_first_qe: DataFrame, bq_client: Client, **context) -> list:
    
    new_client_rows_to_load= []

    for i in range(df_clients_with_first_qe.shape[0]):
        client_id= df_clients_with_first_qe.iloc[i].client_id

        if not client_exists_already(bq_client= bq_client, client_id= client_id, **context):
            opportunity_id= df_clients_with_first_qe.iloc[i].opportunity_id

            client_information_query_path= os.path.join(os.path.dirname(__file__), 'queries', 'get_client_data_ps.sql')

            with open(client_information_query_path, 'r') as client_information_query_file:
                client_information_query= client_information_query_file.read().format(client_id= client_id, opportunity_id= opportunity_id)
            
            bq_job= bq_client.query(query= client_information_query)
            df_client_information= bq_job.to_dataframe()
            
            client_information_results= dict(df_client_information.iloc[-1]) if df_client_information.shape[0] else None

            if client_information_results is not None:
                client_information_results['client_id']= client_id
                client_information_results['creation_datetime_z']= df_clients_with_first_qe.iloc[i].created_at
                client_information_results['registered_datetime_z']= context['data_interval_start']

                new_client_rows_to_load.append(client_information_results)
    
    return new_client_rows_to_load
    
def get_clients_data(**context):

    bq_client = Client(project= context['params'].get('project_id'))
    job_id= context['task_instance'].xcom_pull(task_ids= 'query_first_client_question_events_of_day')
    bq_job= bq_client.get_job(job_id= job_id, location= 'us-central1')

    query_results_df = bq_job.to_dataframe()
    rows_to_load= new_clients_to_load(query_results_df, bq_client, **context)

    if len(rows_to_load):
        load_job= bq_client.load_table_from_dataframe(
            dataframe= DataFrame(
                data= rows_to_load
            ),
            destination= f"{context['params'].get('project_id')}.{context['params'].get('mudata_raw')}.clients_creation",
        )

        return load_job.job_id



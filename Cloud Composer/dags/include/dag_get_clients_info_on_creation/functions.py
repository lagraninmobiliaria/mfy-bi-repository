import os
from textwrap import dedent
from google.cloud.bigquery import Client


def client_exists_already(bq_client: Client, client_id, **context):
    
    query= dedent(f"""
        SELECT EXISTS (
            SELECT
                client_id
            FROM `{context['params'].get('project_id')}.{context['params'].get('mudata_raw')}.clients_creation` clients_creation
            WHERE 
                clients_creation.client_id = {client_id}
        );
    """)
    
    bq_job= bq_client.query(
        query= query
    )
    exists = len(list(bq_job.result())) >= 1

    print(exists)

    return exists

def get_clients_data(**context):

    bq_client = Client(project= context['params'].get('project_id'))
    job_id= context['task_instance'].xcom_pull(task_ids= 'query_first_client_question_events_of_day')
    bq_job= bq_client.get_job(job_id= job_id, location= 'us-central1')

    query_results_df = bq_job.to_dataframe()

    for i in range(query_results_df.shape[0]):
        client_id= query_results_df.iloc[i].client_id
        print(
            query_results_df.iloc[i]
        )

        if not client_exists_already(bq_client= bq_client, client_id= client_id, **context):
            opportunity_id= query_results_df.iloc[i].opportunity_id

            client_information_query_path= os.path.join(os.path.dirname(__file__), 'queries', 'get_client_data_ps.sql')

            with open(client_information_query_path, 'r') as client_information_query_file:
                client_information_query= client_information_query_file.read().format(client_id= client_id, opportunity_id= opportunity_id)
            
            bq_job= bq_client.query(query= client_information_query)
            client_information_results= list(bq_job.result()) if (bq_job.result().num_results >= 1) else None

            if client_information_results is not None:
                print(client_information_results[-1], len(client_information_results), sep='\n')
        
        else:
            print("Client already exists")

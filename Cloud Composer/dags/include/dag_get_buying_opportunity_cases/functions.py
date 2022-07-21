import os

from time import perf_counter

from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, CreateDisposition

class DAGQueriesManager:
    def __init__(self) -> None:
        
        get_buying_opportunity_cases_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            'get_buying_opportunity_cases.sql'
        )
        with open(get_buying_opportunity_cases_query_path, 'r') as sql_file:
            self.get_buying_opportunity_cases_query_template= sql_file.read()
        
        get_client_ticket_id_query_path= os.path.join(
            os.path.dirname(__file__),
            'queries',
            'get_client_ticket_id.sql'
        )
        with open(get_client_ticket_id_query_path, 'r') as sql_file:
            self.get_client_ticket_id_query_template= sql_file.read()

def get_ticket_id_for_buying_opportunity_cases(**context):

    bq_client= Client(project= context['params'].get('project_id'), location= 'us-central1')
    queries_manager= DAGQueriesManager()

    start= perf_counter()
    bq_job_id_buying_op_cases= context['task_instance'].xcom_pull('get_buying_opportunity_cases')
    bq_job_buying_op_cases= bq_client.get_job(job_id= bq_job_id_buying_op_cases)
    df_buying_op_cases= bq_job_buying_op_cases.to_dataframe()
    stop= perf_counter()
    print(f"Get buying opportunity cases takes: {stop - start:0.4f} seconds")

    client_ids= ','.join([str(client_id) for client_id in df_buying_op_cases.client_id.unique()])

    start= perf_counter()
    get_client_ticket_id_query= queries_manager.get_client_ticket_id_query_template.format(
        project_id= context['params'].get('project_id'),
        dataset_id= context['params'].get('mudata_raw'),
        client_ids= client_ids,
    )
    df_tickets_creation= bq_client.query(
        query= get_client_ticket_id_query
    ).to_dataframe()

    print(df_tickets_creation.shape)

    stop= perf_counter()
    print(f"Getting ticket_ids takes: {stop - start:0.4f} seconds")

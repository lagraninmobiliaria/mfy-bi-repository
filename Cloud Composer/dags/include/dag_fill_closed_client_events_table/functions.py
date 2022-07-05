from google.cloud.bigquery import Client, LoadJobConfig, CreateDisposition, WriteDisposition

def load_inferred_closed_client_events(**context):
    
    bq_client= Client(project= context['params'].project_id)
    bq_job_id= context['task_instance'].xcom_pull('get_inferred_closed_client_events')
    bq_job= bq_client.get_job(job_id= bq_job_id)

    df_iferred_closed_client_events= bq_job.to_dataframe()
    total_rows= df_iferred_closed_client_events.shape[0]
    
    print(
        f"Number of iferred closed_client_events to be load: {total_rows}"
    )

    load_job= bq_client.load_table_from_dataframe(
        dataframe= df_iferred_closed_client_events, 
        destination= f"{context['params'].get('project_id')}.{context['params'].get('mudata_raw')}.closed_client_events",
        job_config= LoadJobConfig(
            create_disposition= CreateDisposition.CREATE_NEVER,
            write_disposition= WriteDisposition.WRITE_APPEND
        )
    )
    
    return load_job.job_id
from google.cloud.bigquery import Client, CopyJobConfig, WriteDisposition, CreateDisposition


def transfer_data(table_id, **context):
    
    bq_client= Client(
        project= 'infrastructure-lgi', 
        location= 'us-central1'
    )
    source_table_id = f"{context['params']['prod_params']['project_id']}.{context['params']['prod_params']['mudata_raw']}.{table_id}"
    destination_table_id = f"{context['params']['stg_params']['project_id']}.{context['params']['stg_params']['mudata_raw']}.{table_id}"

    job = bq_client.copy_table(
        sources= source_table_id, 
        destination= destination_table_id,
        location= 'us-central1', 
        job_config= CopyJobConfig(
            create_disposition= CreateDisposition.CREATE_IF_NEEDED,
            write_disposition= WriteDisposition.WRITE_TRUNCATE
        )
    )
    job.result()  # Wait for the job to complete.

    print("A copy of the table created.")

    return job.job_id
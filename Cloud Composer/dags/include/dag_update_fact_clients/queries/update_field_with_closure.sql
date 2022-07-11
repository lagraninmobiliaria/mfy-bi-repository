UPDATE {project_id}.{dataset_id}.fact_clients 
SET  
    is_active= false,
    to_datetime_z= DATETIME('{to_datetime_z}')
WHERE
        client_id= {client_id}
    AND to_datetime_z IS NULL
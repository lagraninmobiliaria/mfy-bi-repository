SELECT
    fact_clients.client_id client_id
    
FROM `{project_id}.{dataset_id}.fact_clients` fact_clients
WHERE
    fact_clients.client_id IN ({list_client_ids})
    

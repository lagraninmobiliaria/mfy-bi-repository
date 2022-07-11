SELECT
    *

FROM `{project_id}.{dataset_id}.fact_clients` fact_clients

WHERE
    fact_clients.client_id= {client_id}

ORDER BY
    fact_clients.from_datetime_z DESC

LIMIT 1
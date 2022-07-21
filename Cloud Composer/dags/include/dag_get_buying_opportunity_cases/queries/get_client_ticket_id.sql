SELECT
    tickets_creation.ticket_id AS ticket_id

FROM `{project_id}.{dataset_id}.tickets_creation` tickets_creation
WHERE
    tickets_creation.client_id = {client_id}

LIMIT 1
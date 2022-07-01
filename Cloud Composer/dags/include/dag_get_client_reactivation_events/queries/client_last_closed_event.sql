SELECT
    closed_client_events.client_id          client_id,
    MAX(closed_client_events.event_id)      event_id,
    MAX(closed_client_events.created_at)    created_at

FROM `{project_id}.{dataset_id}.closed_client_events` closed_client_events
WHERE
        closed_client_events.client_id  = {client_id}
    AND closed_client_events.created_at < TIMESTAMP('{created_at}')

GROUP BY 
    closed_client_events.client_id
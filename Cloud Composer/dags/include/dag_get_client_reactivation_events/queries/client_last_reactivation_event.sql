SELECT
    client_reactivation_events.client_id          client_id,
    MAX(client_reactivation_events.event_id)      event_id,
    MAX(client_reactivation_events.created_at)    created_at

FROM `{project_id}.{dataset_id}.client_reactivation_events` client_reactivation_events
WHERE
        client_reactivation_events.client_id  = {client_id}
    AND client_reactivation_events.created_at < TIMESTAMP('{created_at}')

GROUP BY 
    client_reactivation_events.client_id
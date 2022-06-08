SELECT
    client_reactivation_events.client_id          client_id,
    MAX(client_reactivation_events.event_id)      event_id,
    MAX(client_reactivation_events.created_at)    created_at

FROM `{{ params.project_id }}.{{ params.mudata_raw }}.client_reactivation_events` client_reactivation_events
WHERE
        client_reactivation_events.client_id  = {}
    AND client_reactivation_events.created_at < TIMESTAMP('{}')

GROUP BY 
    client_reactivation_events.client_id
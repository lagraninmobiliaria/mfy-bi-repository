SELECT
    *
FROM `{{ params.project_id }}.{{ params.mudata_raw }}.closed_client_events` closed_client_events
WHERE
        closed_client_events.client_id  = {}
    AND closed_client_events.created_at < TIMESTAMP('{}')
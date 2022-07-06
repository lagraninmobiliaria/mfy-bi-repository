SELECT
    closed_client_events.*,
    'close_client_event' kind

FROM `{{ params.project_id }}.{{ params.mudata_raw }}.closed_client_events` closed_client_events
WHERE
        closed_client_events.created_at >= TIMESTAMP("{{ data_interval_start }}")
    AND closed_client_events.created_at <  TIMESTAMP("{{ data_interval_end }}")

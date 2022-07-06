SELECT
    client_reactivation_events.*,
    'reactivate_client_event' kind

FROM `{{ params.project_id }}.{{ params.mudata_raw }}.client_reactivation_events` client_reactivation_events
WHERE
    client_reactivation_events.created_at >= TIMESTAMP("{{ data_interval_start }}")
    client_reactivation_events.created_at <  TIMESTAMP("{{ data_interval_end }}")

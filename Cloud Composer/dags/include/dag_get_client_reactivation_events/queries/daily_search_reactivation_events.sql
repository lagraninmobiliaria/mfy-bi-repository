SELECT
    *
FROM FROM `{{ params.project_id }}.{{ params.mudata_raw }}.search_reactivation_events` search_reactivation_events
WHERE
        search_reactivation_events.created_at >= TIMESTAMP({{ data_interval_start }})
    AND search_reactivation_events.created_at <  TIMESTAMP({{ data_interval_end }})
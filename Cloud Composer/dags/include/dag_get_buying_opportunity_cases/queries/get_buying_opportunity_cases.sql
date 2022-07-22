SELECT 
    *
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central",
    """
        SELECT
            ee.created_at               AS registered_datetime_z,
            ee.id                       AS event_id,
            ee.opportunity_case_id      AS opportunity_id,
            ee.client_id                AS client_id,
            ee.prop_id                  AS property_id,
            ebce.kind                   AS kind,
            ebce.previous_kind          AS previous_kind

        FROM events_buyingcaseevent ebce
            LEFT JOIN events_event ee
                ON ee.id = ebce.event_ptr_id

        WHERE
            DATE(ee.created_at) = DATE('{{ data_interval_start.date() }}') 
    """
)
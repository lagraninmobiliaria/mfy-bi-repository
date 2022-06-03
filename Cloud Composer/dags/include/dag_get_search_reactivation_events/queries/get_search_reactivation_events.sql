SELECT 
    *
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central",
    """
    SELECT 
        ee.id                       event_id,
        ee.created_at               created_at,
        ee.client_id                client_id

    FROM events_searchcasestatuschangeevent escsce

    LEFT JOIN events_event ee
        ON ee.id = escsce.event_ptr_id

    WHERE
            escsce.new_status = true
        AND ee.polymorphic_ctype_id = {{ params.polymorphic_ctype_id }}
        AND DATE(ee.created_at) = {{ ds }}
    """
)
SELECT 
    *
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us/connections/mudafy", 
    """
    SELECT
        ee.id                       event_id,
        ee.created_at               created_at,
        ee.client_id                client_id,
        ee.opportunity_case_id      opportunity_id

    FROM events_questionevent eqe
        LEFT JOIN events_event ee
            ON ee.id = eqe.event_ptr_id
    WHERE
        DATE(ee.created_at) = DATE('{{ ds }}')
    """
)
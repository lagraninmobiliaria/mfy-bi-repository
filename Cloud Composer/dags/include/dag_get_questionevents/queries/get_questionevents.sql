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

    FROM events_event ee
    
    WHERE
            ee.polymorphic_ctype_id = 42
        AND DATE(ee.created_at) = DATE('{{ ds }}')
    """
)
SELECT 
    *
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us/connections/mudafy", 
    """
    SELECT
        ee.id                       event_id,
        ee.created_at               created_at,
        ee.client_id                client_id

    FROM events_event ee
    
    WHERE
            DATE(ee.created_at) = DATE('{{ ds }}')
        AND ee.polymorphic_ctype_id = {{ polymorphic_ctype_id }}
    """
)
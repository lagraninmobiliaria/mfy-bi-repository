SELECT 
    *
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central",
    """
    SELECT
        *
    FROM properties_property pp
    WHERE
        pp.source_kind = '{}'
    """
)
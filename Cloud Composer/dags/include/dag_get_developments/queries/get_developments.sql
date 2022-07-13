SELECT 
    *
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central",
    """
    SELECT
        {string_for_select}
    FROM properties_developments
    """
)
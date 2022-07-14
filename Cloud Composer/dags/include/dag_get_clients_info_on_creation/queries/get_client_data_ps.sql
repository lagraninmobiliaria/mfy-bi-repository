SELECT 
    * 
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central", 
    """
    SELECT 
        us.first_name   AS name,
        us.email        AS email,
        prof.phone      AS phone,
        oc.country      AS country_code_iso2

    FROM accounts_profile prof
        LEFT JOIN auth_user us
            ON us.id = prof.user_id
        LEFT JOIN accounts_opportunitycase oc
            ON oc.client_id = prof.id

    WHERE 
            prof.id = {client_id}
        AND oc.id   = {opportunity_id}
    """
);
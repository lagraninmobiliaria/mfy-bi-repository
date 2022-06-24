SELECT 
    * 
FROM EXTERNAL_QUERY(
    "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central", 
    """
    SELECT 
        us.first_name   name,
        us.email        email,
        prof.phone      phone,
        oc.country      country

    FROM accounts_profile as prof
        LEFT JOIN auth_user us
            ON us.id = prof.user_id
        LEFT JOIN accounts_opportunitycase oc
            ON oc.client_id = prof.id

    WHERE 
            prof.id = {client_id}
        AND oc.id   = {opportunity_id}
    """
);
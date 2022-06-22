SELECT
    properties.id       property_id,
    NULL                property_unique_id,
    source              external_source_id,
    street              street,
    street_number       street_number,
    address             address,
    latitude            latitude,
    longitude           longitude,
    location_id         location_id,
    country             country_id,
    title               title,
    source_kind         source,
    property_kind       kind,
    price               listed_price,
    valuation_price     appraisal_price,
    currency            currency,
    operation_kind      operation_kind,
    opertion_status     opertion_status,
    slug                link,
    listed              is_active,
    NULL                listed_date,
    NULL                unlisted_date,
    commercial_agent_id commercial_advisor_id,
    producer_agent_id   producer_advisor_id,
    client_id           client_id,
    NULL                alliance_id,
    development_id      development_id,
    STRUCT(
        NULL as empty
    )                   prop_additionals                              

FROM `{{ params.project_id }}.{{ params.mudata_raw }}.properties` properties
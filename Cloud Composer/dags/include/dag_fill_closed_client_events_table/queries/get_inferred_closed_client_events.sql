WITH closed_clients_without_closure AS (
  SELECT * FROM EXTERNAL_QUERY(
  "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central", 
  '''
    SELECT
      DISTINCT cp.profile_ptr_id client_id
    FROM accounts_clientprofile cp
    WHERE 
      NOT EXISTS(
        SELECT
          ee.client_id 
        FROM events_event ee
        WHERE
              ee.polymorphic_ctype_id= 120 
          AND ee.client_id = cp.profile_ptr_id
      ) 
      AND cp.buying_stage = 'closed'
    ;'''
  )
), closed_clients_last_closed_case AS (
  SELECT * FROM EXTERNAL_QUERY(
  "projects/infrastructure-lgi/locations/us-central1/connections/mudafy-prod-replic-us-central", 
  '''
    SELECT
      oc.client_id          client_id,
      MAX(ee.id)            event_id,
      MAX(ee.created_at)    created_at

    FROM events_buyingcaseevent ebce
      LEFT JOIN events_event ee
        ON ebce.event_ptr_id = ee.id
      LEFT JOIN accounts_buyinglead bl
        ON bl.opportunitycase_ptr_id = ee.opportunity_case_id
      LEFT JOIN accounts_opportunitycase oc
        ON oc.id = bl.opportunitycase_ptr_id
      LEFT JOIN accounts_clientprofile cp
        ON cp.profile_ptr_id= oc.client_id
      
    WHERE
          ebce.kind= 'closed'
      AND cp.buying_stage= 'closed'

    GROUP BY oc.client_id
    ;'''
  )
)
SELECT
  closed_clients_without_closure.client_id,
  closed_clients_last_closed_case.event_id, 
  closed_clients_last_closed_case.created_at

FROM closed_clients_without_closure 
  LEFT JOIN closed_clients_last_closed_case
    ON closed_clients_last_closed_case.client_id = closed_clients_without_closure.client_id

WHERE 
    NOT EXISTS(
      SELECT
        event_id
      FROM `{project_id}.{dataset_id}.{table_id}` closed_client_events
      WHERE 
        closed_client_events.event_id = closed_clients_last_closed_case.event_id
  )
  
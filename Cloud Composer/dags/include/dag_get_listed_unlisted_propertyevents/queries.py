from textwrap import dedent 

def listed_and_unlisted_propertyevents(date: str = '2021-5-4') -> str:
    return dedent(
        f'''
        SELECT * 
        FROM EXTERNAL_QUERY("projects/infrastructure-lgi/locations/us/connections/mudafy", 
            """
                SELECT
                    ee.created_at   created_at,
                    ee.prop_id      prop_id,
                    epe.kind        kind

                FROM events_propertyevent epe
                    LEFT JOIN events_event ee
                        ON epe.event_ptr_id = ee.id

                WHERE
                        epe.kind IN ('listed', 'unlisted')
                    AND DATE(ee.created_at) = DATE('{date}')
            """
        );
        '''
    )
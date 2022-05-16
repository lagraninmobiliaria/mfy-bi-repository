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

def get_daily_propertyevents(project_id: str = 'infrastructure-lgi', dataset: str = 'stg_mudata_raw', date: str = '2021-5-4') -> str:
    return dedent(
        f'''
        SELECT 
            listed_and_unlisted_propertyevents.created_at   created_at,
            listed_and_unlisted_propertyevents.prop_id      prop_id,
            listed_and_unlisted_propertyevents.kind         kind

        FROM `{project_id}.{dataset}.listed_and_unlisted_propertyevents` listed_and_unlisted_propertyevents
        WHERE
            DATE(listed_and_unlisted_propertyevents.created_at) = DATE("{date}")
        '''
    )

def get_properties_daily_net_propertyevents(project_id: str = 'infrastructure-lgi', dataset: str = 'stg_mudata_curated', date: str = '2021-5-4') -> str:
    return dedent(
        f'''
        SELECT
            *
        FROM `{project_id}.{dataset}.properties_daily_net_propertyevents` properties_daily_net_propertyevents
        WHERE 
            properties_daily_net_propertyevents.registered_date = DATE("{date}")
        '''
    )

def test_bquery():
    return dedent(
        f'''
        SELECT *
        FROM `infrastructure-lgi.DW_Mudafy.LKP_Clientes`
        LIMIT 50
        '''
    )

if __name__ == '__main__':
    print(get_daily_propertyevents())
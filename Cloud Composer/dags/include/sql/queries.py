from datetime import date
from textwrap import dedent, wrap

def listed_and_unlisted_propertyevents(date: str = '2021-5-4') -> str:
    return dedent(
        f'''
        SELECT * 
        FROM EXTERNAL_QUERY("projects/infrastructure-lgi/locations/us/connections/mudafy", 
            """
                SELECT
                    ee.created_at   event_created_at,
                    ee.prop_id      prop_id,
                    epe.kind        event_kind

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


def test_bquery():
    return dedent(
        f'''
        SELECT *
        FROM `infrastructure-lgi.DW_Mudafy.LKP_Clientes`
        LIMIT 50
        '''
    )

# if __name__ == '__main__':
#     print(test_bquery())
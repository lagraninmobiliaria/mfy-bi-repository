from datetime import date
from textwrap import dedent
from pendulum import datetime 

def listed_and_unlisted_events(date: str = '2021-5-4') -> str:
    return dedent(
        f"""
            SELECT
                ee.created_at,
                ee.prop_id,
                ee.development_id, 
                epe.kind

            FROM events_propertyevent epe
                LEFT JOIN events_event ee
                    ON epe.event_ptr_id = ee.id

            WHERE
                    epe.kind IN ('listed', 'unlisted')
                AND DATE(ee.created_at) = DATE('{date}')
        """
    )

# if __name__ == '__main__':
#     print(listed_and_unlisted_events())
from textwrap import dedent
from pendulum import datetime

def get_client_first_questionevent(data_interval_start= None, data_interval_end= None):
    
    return dedent(
        f'''
        SELECT
            * 
        FROM EXTERNAL_QUERY(
            "projects/infrastructure-lgi/locations/us/connections/mudafy", 
            """
            WITH v_client_first_questionevent AS (
                SELECT
                    ee.client_id        client_id,
                    MIN(ee.id)          event_id,
                    MIN(ee.created_at)  event_created_at

                FROM events_questionevent eqe
                LEFT JOIN events_event ee
                    ON ee.id = eqe.event_ptr_id

                GROUP BY
                    ee.client_id

                HAVING 
                        MIN(ee.created_at) >= TO_TIMESTAMP('{str(data_interval_start).replace('T', ' ')[:-6]}', 'YYYY-MM-DD HH24:MI:SS')
                    AND MIN(ee.created_at) <  TO_TIMESTAMP('{str(data_interval_end).replace('T', ' ')[:-6]}', 'YYYY-MM-DD HH24:MI:SS')
            )
            SELECT
                v_client_first_questionevent.*                ,
                ee.opportunity_case_id          opportunity_id

            FROM v_client_first_questionevent
                LEFT JOIN events_event ee
                    ON ee.id = v_client_first_questionevent.event_id
            """
        );
        '''
    )

if __name__ == "__main__":
    data_interval_start = datetime(2021, 6, 27, 18, 0, 0, 0)
    data_interval_end = datetime(2021, 6, 27, 18, 30, 0, 0)
    print(get_client_first_questionevent(data_interval_start, data_interval_end))
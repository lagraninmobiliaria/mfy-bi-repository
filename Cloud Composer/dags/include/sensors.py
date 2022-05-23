from datetime import datetime

from airflow.sensors.base import BaseSensorOperator
from pendulum import date

class FirstDAGRunSensor(BaseSensorOperator):

    def __init__(self, date, *, poke_interval: float = 60, timeout: float = 60 * 60 * 24 * 7, soft_fail: bool = False, mode: str = 'poke', exponential_backoff: bool = False, **kwargs) -> None:
        
        super().__init__(poke_interval=poke_interval, timeout= timeout, soft_fail= soft_fail, mode= mode, exponential_backoff=exponential_backoff, **kwargs)
        self.ds = date
        self.dag = kwargs['dag']

    def poke(self, **context):
        return datetime.strptime(self.ds, '%Y-%m-%d').date() == self.dag.start_date.date()
    
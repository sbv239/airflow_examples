'''
Вывод всего контекста, передаваемого python-функции
'''
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(**kwargs):
    for key, value in kwargs.items():
        print(f'{key!r}: {value!r}')


dag_params = {
    'dag_id': 'xxa02-context',
    'description': __doc__,
    'start_date': datetime.datetime(2023, 6, 18),
    'schedule_interval': datetime.timedelta(days=1),
    'default_args': {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
    },
    'catchup': False,
    'tags': ['xxa'],
}

with DAG(**dag_params) as dag:
    PythonOperator(
        task_id='python-func-context',
        python_callable=print,
    )

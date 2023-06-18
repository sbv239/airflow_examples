'''
Первый DAG на AirFlow 2.2.4.

Выполняет команду `pwd` в рамках `BashOperator`,
а затем распечатывает шаблонизируемую jinja 
переменную `ds` из `PythonOperator`.
'''
import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds, **kwargs):
    print(ds)
    print('Variables passed in to the function:', kwargs)


dag_params = {
    'dag_id': 'xxa01-hello-dag',
    'description': __doc__ + ' [description]',  # Высплывающая подсказка?
    'doc_md': __doc__,  # Подробное описание графа?
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
    pwd = BashOperator(
        task_id='pwd',
        bash_command='pwd',
        #env={},
        #append_env=True,
    )
    ds = PythonOperator(
        task_id='ds',
        python_callable=print_ds,
    )
    pwd >> ds

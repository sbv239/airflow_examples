from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def python_operator_test(ds, **kwargs):
    print(f'ds={ds}')	


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

schedule_interval = '0 11 * * *'

dag = DAG('hw_a-ivanov-41_2', default_args=default_args, schedule_interval=schedule_interval)

t1 = BashOperator(task_id="bash_operator_test",
                  bash_command="pwd ",
                  dag=dag)

t2 = PythonOperator(task_id='python_operator_test',
                    python_callable=python_operator_test,
                    dag=dag)

t1 >> t2
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_time(ds, **kwargs):
    print(ds)
    return 0


with DAG(
        'hw_l_kobelev_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }
) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_time',
        python_callable=print_time,
    )

    t1 >> t2

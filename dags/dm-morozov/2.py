from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_logic_date(ds):
    print(ds)


with DAG(
    '2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
) as dag:
    t1 = BashOperator(
        task_id='pwd',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='logic_date',
        python_callable=print_logic_date
    )

    t1 >> t2

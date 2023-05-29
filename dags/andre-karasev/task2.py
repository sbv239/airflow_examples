from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator, PythonOperator
from airflow import DAG

date = "{{ ds }}"


def print_date(ds):
    print(ds)


with DAG(
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
) as dag:
    bash = BashOperator(
        task_id='python_pwd',
        bash_command='pwd',
    )
    operator = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    bash >> operator

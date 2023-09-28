from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_g-vinokurov_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
        description='First DAG in task_1',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 28),
        catchup=False,
) as dag:

    operator_1 = BashOperator(
        task_id='Bash_operator',
        bash_command='pwd',
    )

    def print_ds(ds, **kwargs):
        print(ds)

    operator_2 = PythonOperator(
        task_id='Python_operator',
        python_callable=print_ds
    )

    operator_1 >> operator_2
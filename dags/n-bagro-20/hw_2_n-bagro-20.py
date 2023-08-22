from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

with DAG (
    "hw_2_n-bagro-20",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'lesson_11_step_2',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 8, 21),
    catchup = False,
) as dag:
    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'
    )
    def print_context(ds):
        print(ds)

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_context
    )

    t1 >> t2

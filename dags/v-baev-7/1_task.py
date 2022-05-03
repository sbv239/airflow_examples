from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    "hw_1_v-baev-7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG for 1_task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=5, day=3),
    catchup=False,
    tags=['hw_1'],
) as dag:
        t1 = BashOperator(
                task_id='show_current_folder',
                bash_command='pwd'
        )

def print_context(ds, **kwargs):
        print(ds)

t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context
)
t1 >> t2
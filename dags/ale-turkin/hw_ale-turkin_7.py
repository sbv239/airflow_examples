
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

with DAG(
    'hw_ale-turkin_7',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'dag_ex_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags = ['example']
    ) as dag:

    def print_context(ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        
    for i in range(20):
        task = PythonOperator(
        task_id = 'print_context' + str(i),
        python_callable = print_context,
        op_kwargs={'task_number': {i}}
        )
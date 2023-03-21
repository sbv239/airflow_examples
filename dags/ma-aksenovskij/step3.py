from airflow import DAG
from airflow.operator.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


with DAG('step3_dag', default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}, start_date=datetime(2023,1,1)) as dag:

    def prekol(number):
        print(f'number of task: {number}')

    for i in range(30):
        if i < 10:
            t1 = BashOperator(
                task_id = 'bash_operator',
                bash_command = f'echo {i}'
            )
        else:
            t2 = PythonOperator(
                task_id = 'python_operator',
                python_callable = prekol,
                op_kwargs = {'number': i}

            )
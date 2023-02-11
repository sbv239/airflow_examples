from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_i-kulikov-17',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Second ex with 30 iterations',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 10),
    catchup=False,
    tags=['hw_3_lawfirmfacc@mail.ru'],

) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='bash_' + str(i),
            bash_command=f'echo {i}',
        )

    def print_context(ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(ts, run_id)

    for i in range(20):
        t1 = PythonOperator(
            task_id=f'print_task_{i}',
            python_callable=print_context,
            op_kwargs={'task_number': i}
        )

    t1
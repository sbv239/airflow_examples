from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    's-selunskij-7_task_2',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),

    },
    description='a.burlakov-9_DAG_task_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 3),
    catchup=False,
    tags=['task_2']

,
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )


    def get_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id = 'task_number'+str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number': i}
        )

t1 >> t2
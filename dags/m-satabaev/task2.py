from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'murad_satabaev_second_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='murad_satabaev_first_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 10),
        catchup=False,
        tags=['murad_tag'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i}',
            bash_command=f'echo {i}'
        )

    def all_tasks(task_number):
        print(f'task number is: {task_number}')

    for k in range(20):
        t2 = PythonOperator(
            task_id=f'print_task_{k}',
            python_callable=all_tasks,
            op_kwargs={'task': k}
        )
    t1 >> t2

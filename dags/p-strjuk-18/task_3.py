from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

def print_task(task_number):
    print(f'task number is: {task_number}')
    return 'Home' + str(task_number)

with DAG(
    'task_3_stryuk',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='dag for task 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,4,2),
    catchup=False,
    tags=['task_3', 'stryuk']
) as dag:
    for i in range(1, 11):
        bash_task = BashOperator(
            task_id = f'bash_task_{i}',
            bash_command = f'echo {i}',

        )
    for i in range(11, 31):
        pyth_task = PythonOperator(
            task_id = f'pyth_task_{i}',
            python_callable=print_task,

            op_kwargs={'task_number': i}
        )

    dag.doc_md = dedent("""
    #ПРИВЕТ
    Это `дока` к *жирному* и **курсовному** ***дагу***
    """)
    bash_task >> pyth_task
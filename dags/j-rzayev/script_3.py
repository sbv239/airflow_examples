from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    'Print_30_args',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Print 30 args',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 18),
    catchup=False,
    tags=['task_3', 'lesson_11']
)

tasks = []

for i in range(10):
    t1 = BashOperator(
        task_id=f'hw_j-rzayev_{i}',
        bash_command=f'echo {i}',
        dag=dag,
    )
    tasks.append(t1)

def print_i(task_number ):
    print(f"task number is: {task_number}")

for task_number in range(10, 30):
    t2 = PythonOperator(
        task_id=f'hw_j-rzayev_{task_number}',
        python_callable=print_i,
        op_kwargs={'task_number': task_number},
    )
    tasks.append(t2)

for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]

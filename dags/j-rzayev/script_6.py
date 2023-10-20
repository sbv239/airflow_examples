from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    'j-rzayev_task_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=['task_6', 'lesson_11', 'j-rzayev']
)

tasks = []

for i in range(10):
    t1 = BashOperator(
        task_id=f'hw_j-rzayev_{i}',
        bash_command=f'echo $NUMBER',
        env={'NUMBER': i},
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

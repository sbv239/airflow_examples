from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def python_task(task_number: int):
    print(f'task number is: {task_number}')

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hw_3_a-buldakov',
    default_args = default_args,
    start_date = datetime.now(),
    tags=['a-buldakov']
)

for i in range(30):
    if i < 10:
        bash_task = BashOperator(
            task_id=f'bash_task_{i}',
            bash_command=f'echo "Bash task {i}"',
            dag=dag
        )
        bash_task
    else:
        python_tp = PythonOperator(
            task_id=f'python_task_{i}',
            python_callable=python_task,
            op_kwargs={'task_number': i},
            provide_context=True,
            dag=dag
        )
        python_task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

def python_task(task_number, ts, run_id):
    print(f'task number is: {task_number}')
    print(ts)
    print(run_id)

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hw_7_a-bosikov',
    default_args = default_args,
    start_date = datetime.now(),
    tags=['a-bosikov']
)

for i in range(20):
    python_task_op = PythonOperator(
        task_id=f'python_task_{i}',
        python_callable=python_task,
        op_kwargs={'task_number': i},
        provide_context=True,
        dag=dag,
    )
    python_task
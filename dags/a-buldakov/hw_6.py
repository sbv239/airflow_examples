from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hw_6_a-buldakov',
    default_args = default_args,
    start_date = datetime.now(),
    tags=['a-buldakov']
)

for i in range(10):
    bash_task = BashOperator(
            task_id=f'bash_task_{i}',
            bash_command=f'echo $NUMBER',
            env={'NUMBER': i},
            dag=dag
        )
    bash_task
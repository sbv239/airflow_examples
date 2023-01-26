from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'v-susanin_task_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='v-susanin_DAG_task_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 26),
    catchup=False,
    tags=['DAG_task_3'],

) as dag:
    for i in range(10):
        first=BashOperator(
            task_id='echo'+str(i),
            bash_command=f"echo {i}")

    first.doc_md = dedent(
        """\
        #### Task Documentation
        Динамические задачи. 
        """
    )

    def get_task_number(task_number):
        print(f"task number is: {task_number}")
    for i in range(20):
        second=PythonOperator(
            task_id='task_number'+str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number':i})
first >> second

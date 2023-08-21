from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_p-evdokimov_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_p-evdokimov_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_p-evdokimov_5'],
) as dag:
    ts = "{{ ts }}"
    run_id = "{{ run_id }}"
    for i in range(5):
        t1 = BashOperator(
            task_id='counter_num_' + str(i),
            bash_command= f'echo {ts}\
                echo {run_id}',
        )
        # t1.doc_md = dedent(
        # """
        # #### paragraph
        # `code` **bald** _italic_ 
        # """
        # )
t1 
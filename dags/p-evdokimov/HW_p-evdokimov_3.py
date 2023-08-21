from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_p-evdokimov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_p-evdokimov_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_p-evdokimov_3'],
) as dag:
    def print_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(10):
        t1 = BashOperator(
            task_id='counter_num_' + str(i),
            bash_command=f'echo {i}',
        )
        t1.doc_md = dedent(
        """
        #### paragraph
        `code` *bald* _italic_ 
        """
        ) 
    for i in range(20):
        t2 = PythonOperator(
            task_id='task_num_' + str(i),
            python_callable=print_number,
            op_kwargs={'task_number': i}
        )
        t1.doc_md = dedent(
        """
        #### paragraph
        `code` *bald* _italic_ 
        """
        )
t1 >> t2
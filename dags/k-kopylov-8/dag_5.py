from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG('kkopylov_dag_5',
    default_args = default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = 't_1_echo_'+str(i),
            bash_command = f'echo $NUMBER',
            env = {"NUMBER" : "i"})
        t1.doc_md = dedent('''
        #### Big ass
        `t1` **documentation** with *italic* text
        ''')
        t1


         
         

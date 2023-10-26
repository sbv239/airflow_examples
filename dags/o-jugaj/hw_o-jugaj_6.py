from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_o-jugaj_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
        description='hw_o-jugaj_6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 23),
        catchup=False,
        tags=['hw_o-jugaj_6'],
    ) as dag:

        for i in range(1,11):
            t1 = BashOperator(  
                task_id='task_' + str(i),
                bash_command='echo $NUMBER',
                env={'NUMBER':str(i)}
            )
        
        t1.doc_md = dedent(
            """\
            ### Task Documentation
            #*курсив*
            #**жирный**
            #`code`
            """
        )
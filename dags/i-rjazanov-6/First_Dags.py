from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        # Название таск-дага
        'ryazanov_task_1',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 1',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_1']

) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command = 'pwd'
    )

    t1.doc_md = dedent(
        '''
        ## Documentation
        Print directory `PWD` Airflow
        '''
    )

    def print_txt(ds):
        print(ds)
        return 'This is task print txt'

    t2 = PythonOperator(
        task_id='print_txt',
        python_callable=print_txt
    )

    t1 >> t2
#%%

"""
hw_12_m_zharehina_5
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook

from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


###################


def func():
    from airflow.models import Variable
    if Variable.get('is_startml'):
        return 'startml_desc'
    return 'not_startml_desc'


with DAG(
        'hw_12_m_zharehina_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 26),
        catchup=False,
) as dag:
    
    t1 = DummyOperator(task_id='bef_branch',)

    t2 = BranchPythonOperator(task_id='determ',
                        provide_context=True,
                        python_callable=func,)

    t3 = BashOperator(task_id='startml_desc',
                        bash_command='echo "StartML is a starter course for ambitious people"',)

    t4 = BashOperator(task_id='not_startml_desc',
                        bash_command='echo "Not a startML course, sorry"',)

    t5 = DummyOperator(task_id='aft_branch',)

    t1 >> t2 >> [t3, t4] >> t5
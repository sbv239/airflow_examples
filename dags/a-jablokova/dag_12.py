from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


with DAG(
    'intro_12th',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='intro_12th',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 10),
    catchup=False,
    tags=['a-jablokova'],
) as dag:

    start = DummyOperator(
        task_id = 'start'
    )

    def _choose_branch():
        is_startml = Variable.get('is_startml')
        if is_startml:
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    choose_branch = BranchPythonOperator(
        task_id = 'choose_branch',
        python_callable = _choose_branch,
    )

    def startml_desc():
        print('StartML is a starter course for ambitious people')

    startml =  PythonOperator(
        task_id = 'startml_desc',
        python_callable = startml_desc,
    )

    def not_startml_desc():
        print('Not a startML course, sorry')

    not_startml =  PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = not_startml_desc,
    )

    end = DummyOperator(
        task_id = 'end'
    )

    start >> choose_branch >> [startml, not_startml] >> end
    #                           -->    startml   --
    #                          |                   |
    # start -> choose_branch --                    |--> end
    #                          |                   | 
    #                           -->  not_startml --




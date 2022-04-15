from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
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

    def choose_branch():
        is_startml = Variable.get("is_startml")
        print(f'is_startml: {is_startml}')
        print(type(is_startml))
        if is_startml:
            return "startml_desc"
        else:
            return "not_startml_desc"

    choose_course = BranchPythonOperator(
        task_id = 'choose_branch',
        python_callable = choose_branch,
    )

    startml =  BashOperator(
        task_id = 'startml_desc',
        bash_command = 'echo "StartML is a starter course for ambitious people"',
    )

    not_startml =  BashOperator(
        task_id = 'not_startml_desc',
        bash_command = 'echo "Not a startML course, sorry"',
    )

    end = DummyOperator(
        task_id = 'end'
    )

    start >> choose_course >> [startml, not_startml] >> end
    #                           -->    startml   --
    #                          |                   |
    # start -> choose_branch --                    |--> end
    #                          |                   | 
    #                           -->  not_startml --

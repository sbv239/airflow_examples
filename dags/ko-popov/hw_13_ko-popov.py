"""
Создайте DAG, имеющий BranchPythonOperator.
Логика ветвления должна быть следующая:
если значение Variable is_startml равно "True", то перейти в таску с task_id="startml_desc",
иначе перейти в таску с task_id="not_startml_desc".
Затем объявите две задачи с task_id="startml_desc" и task_id="not_startml_desc".

"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import timedelta, datetime
from psycopg2.extras import RealDictCursor
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

def choose_task():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml == "True":   #переменные Airflow всегда возвращаются как строки, поэтому мы сравниваем is_startml со строкой 'True', а не с булевым значением True
        return "startml_desc"
    else:
        return "not_startml_desc"

def print_startml():
    print("StartML is a starter course for ambitious people")

def print_not_startml():
    print("Not a StartML couse, sorry")

with DAG(
    'hw_13_ko-popov',
    default_args={
        'depends_on_past': False,
        'email': {'mdkonstantinp@gmail.com'},
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hw_13_ko-popov dag',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 5, 29),
    catchup=False,
    tags = ['hw_13_ko-popov'],
) as dag:
    dummy_start = DummyOperator(task_id="dummy_start")
    dummy_end = DummyOperator(task_id="dummy_end")

    branch = BranchPythonOperator(
        task_id = "branching",
        python_callable = choose_task,
    )

    startml_desc = PythonOperator(
        task_id = "startml_desc",
        python_callable = print_startml,
    )

    not_startml_desc = PythonOperator(
        task_id = "not_startml_desc",
        python_callable = print_not_startml,
    )

    dummy_start >> branch >> [startml_desc, not_startml_desc] >> dummy_end
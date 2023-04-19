from datetime import datetime, timedelta
from textwrap import dedent

from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def get_var():
    from airflow.models import Variable

    var = Variable.get("is_startml")
    print(var)
    return var

with DAG(
    "Variables",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description= "first try of starting DAG's",
    schedule_interval= timedelta(days=1),
    start_date= datetime(2023, 4, 17),
    catchup= False,
    tags=['idk12']
) as dag:

    t1 = PythonOperator(
        task_id="return_var",
        python_callable=get_var
    )

from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def read_var():
    print(Variable.get("is_startml"))

with DAG(
    "task_12_k-zhuravlev",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    start_date=datetime.now(),
    tags=["Cool_tag"]
) as dag:
    t1 = PythonOperator(
        task_id="xcom_pusher",
        python_callable=read_var
    )

    t1
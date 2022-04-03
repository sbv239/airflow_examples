from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

def select_task():
    from airflow.models import Variable

    is_startml = Variable.get("is_startml")
    if is_startml == True:
        return "startml_desc"
    return "not_startml_desc"

def not_startml_desc():
    print("Not a startML course, sorry")
    
def startml_desc():
    print("StartML is a starter course for ambitious people")
    
with DAG(
        'hw_13_d-kizenko-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_13']
) as dag:

    t1 = DummyOperator(
        task_id="before_branching"
    )

    t2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=select_task,
    )

    t3 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml_desc,
    )

    t4 = PythonOperator(
        task_id="startml_desc",
        python_callable=startml_desc,
    )

    t5 = DummyOperator(
        task_id="after_branching",
    )

    t1 >> t2 >> [t3, t4] >> t5
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta


default_args = {'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                }

with DAG(
    'hw_j-kutnjak-21_13',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 30),
    catchup=False,
    tags=['example'],
) as dag:

    def get_branch():
        variable = Variable.get('is_startml')
        if variable == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    def print_startml_desc():
        print("StartML is a starter course for ambitious people")

    def print_not_startml_desc():
        print("Not a startML course, sorry")

    t1 = DummyOperator(
        task_id="before_branching"
    )

    t2 = BranchPythonOperator(
        task_id="branching",
        python_callable=get_branch
    )

    t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=print_startml_desc
    )

    t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_not_startml_desc
    )

    t5 = DummyOperator(
        task_id="after_branching"
    )

    t1 >> t2 >> [t3, t4] >> t5

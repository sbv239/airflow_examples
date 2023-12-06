from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator

def get_variable():
        from airflow.models import Variable
        if Variable.get('is_startml') == 'True':
                return "startml_desc"
        else:
                return "not_startml_desc"



with DAG(
    'm-mihail-24_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example_13'],
) as dag:
        d1 = DummyOperator(task_id="before_branching")

        branch_op = BranchPythonOperator(
                task_id='determine_course',
                python_callable=get_variable
        )

        p1 = PythonOperator(
                task_id="startml_desc",
                python_callable=lambda: print("StartML is a starter course for ambitious people")
        )

        p2 = PythonOperator(
                task_id="not_startml_desc",
                python_callable=lambda: print("Not a startML course, sorry")
        )

        d2 = DummyOperator(task_id="after_branching")

        d1 >> branch_op >> [p1, p2] >> d2
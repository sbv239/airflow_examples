from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from airflow.models import Variable


def get_course():
    is_startml = Variable.get("is_startml")

    if is_startml == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"

def show_startml_desc():
    print("StartML is a starter course for ambitious people")
def show_not_startml_desc():
    print("Not a startML course, sorry")


with DAG(
        "hw_13_n-chuviurova",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="Branching",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 15),
        catchup=False,
        tags=["task_13"],
) as dag:

    t0 = DummyOperator(
        task_id="before_branching",
    )

    t1 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=get_course,
    )

    t2 = PythonOperator(
        task_id="startml_desc",
        python_callable=show_startml_desc,
    )

    t3 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=show_not_startml_desc,
    )

    t4 = DummyOperator(
        task_id="after_branching"
    )


    t0 >> t1 >> [t2, t3] >> t4

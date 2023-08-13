from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

with DAG(
        "hw_13_s-kim",
        description="Homework 13",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 8, 10),
        catchup=True,
        tags=["s-kim"],
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }
) as dag:

    def condition_01():
        gl_var = Variable.get("is_startml")
        if gl_var == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    def is_start_ml():
        print("StartML is a starter course for ambitious people")

    def not_start_ml():
        print("Not a startML course, sorry")


    t1 = BranchPythonOperator(task_id="condition_01",
                              python_callable=condition_01)

    t2 = PythonOperator(task_id="startml_desc",
                        python_callable=is_start_ml)

    t3 = PythonOperator(task_id="not_startml_desc",
                        python_callable=not_start_ml)

    d1 = DummyOperator(task_id="dummy_01")
    d2 = DummyOperator(task_id="dummy_02")

    d1 >> t1 >> [t2, t3] >> d2
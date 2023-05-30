import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

with DAG(
    'hw_12_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = "Making DAG for 12th task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 26),
    catchup = False,
    tags = ["hw_12_a-maslennikov"],
) as dag:

    def check_var():
        if Variable.get("is_startml") is "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    def startml_desc():
        print("StartML is a starter course for ambitious people")

    def not_startml_desc():
        print("Not a startML course, sorry")

    t1 = DummyOperator(
        task_id = "before_branching"
    )

    t2 = BranchPythonOperator(
        task_id = "branching",
        python_callable = check_var,
    )


    t3 = PythonOperator(
        task_id = "startml_desc",
        python_callable = startml_desc
    )


    t4 = PythonOperator(
        task_id = "not_startml_desc",
        python_callable = not_startml_desc
    )


    t5 = DummyOperator(
        task_id = "after_branching"
    )

    t1 >> t2 >> [t3, t4] >> t5
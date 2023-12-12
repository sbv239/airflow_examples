from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

with DAG(
    "hw_13_n_vojtova",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Dag with branching',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,12,11),
    catchup=False,
    tags=['hw_13_n_vojtova'],
) as dag:
    from airflow.operators.python import BranchPythonOperator

    FIRST_OPT = "startml_desc"
    SECOND_OPT = "not_startml_desc"
    def choose_branch():
        return FIRST_OPT if Variable.get("is_startml") == "True" else SECOND_OPT

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=choose_branch,
    )

    task_a = PythonOperator(
        task_id= FIRST_OPT,
        python_callable=lambda: "StartML is a starter course for ambitious people"
    )

    task_b = PythonOperator(
        task_id=SECOND_OPT,
        python_callable=lambda: "Not a startML course, sorry"
    )

    print_var = PythonOperator(
        task_id="print_var",
        python_callable=lambda: print(Variable.get("is_startml"))
    )

    start >> print_var >> branching >> [task_a,task_b] >>end
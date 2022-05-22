from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
from airflow.models import Variable

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}



with DAG('kkopylov_dag_12',
    default_args=default_args,
    description='A simple tutorial DAG№12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False) as dag:

    def choose_task():
        var = Variable.get("is_startml")
        if var:
            return "startml_desc"
        else:
            return "not_startml_desc"
    def print_sml():
        print("StartML is a starter course for ambitious people")
    def print_not_sml():
        print("Not a startML course, sorry")
    t1 = PythonOperator(
        task_id="before_branching",
        )
    t2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=choose_task
    )
    t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=print_sml
    )
    t3 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_not_sml
    )
    t4 = DummyOperator(
        task_id="after_branching"
    )
    t1>>[t2,t3]>>t4

         
         

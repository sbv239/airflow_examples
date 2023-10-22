from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

with DAG(
    'hw_13_ex_9-n-jazvinskij',
    default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    description = 'hw_13_ex_9',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 10, 21),
    catchup = False,
) as dag:
    def what_will_be():
        is_startml = Variable.get('is_startml')
        if is_startml == 'True':
            return "startml_desc"
        else:
            return "not_startml_desc"

    def start_ml():
        return "StartML is a starter course for ambitious people"
    def not_start_ml():
        return "Not a startML course, sorry"

    t_ml = PythonOperator(
        task_id="startml_desc",
        python_callable = start_ml
    )

    t_not_ml = PythonOperator(
        task_id="not_startml_desc",
        python_callable= not_start_ml
    )

    t1 = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = what_will_be
    )
    t2 = DummyOperator(
        task_id = 'before_branching'
    )

    t3 = DummyOperator(
        task_id='after_branching'
    )

    t2>>t1>>[ t_ml, t_not_ml]>>t3
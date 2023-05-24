from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta


def choose_option():
    if Variable.get('is_startml') == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"
    
def get_startml_description():
    print("StartML is a starter course for ambitious people")

def get_no_startml_description():
    print("Not a startML course, sorry")

with DAG(

    'hw_13_r-muratov-9',

    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    schedule_interval=timedelta(days=1),

    start_date=datetime(2023,5,20),

    catchup=False
    ) as dag:

    determine_course = BranchPythonOperator(
        task_id='choose_option',
        python_callable=choose_option
    )

    option_1 = PythonOperator(
        task_id='startml_desc',
        python_callable=get_startml_description
    )

    option_2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=get_no_startml_description
    )

    before_branching = DummyOperator(
        task_id="before_branching"
    )

    after_branching = DummyOperator(
        task_id="after_branching"
    )

    before_branching >> determine_course >> [option_1, option_2] >> after_branching
    
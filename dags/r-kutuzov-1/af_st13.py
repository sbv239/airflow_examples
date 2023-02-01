from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta


def get_task_id():
    is_startml = Variable.get('is_startml')  # необходимо передать имя, заданное при создании Variable
    # теперь в is_startml лежит значение Variable
    if is_startml == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"


def print_context(line):
    print(line)


with DAG(
    'r-kutuzov-1_dag_13-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Airflow lesson step 13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=['r-kutuzov-1_step_13'],
) as dag:
    
    task_start = DummyOperator(
        task_id='before_branching',
    )
    
    task_branch = BranchPythonOperator(
        task_id='determine_course',
        python_callable=get_task_id,
    )
    
    task_true = PythonOperator(
        task_id="startml_desc", 
        python_callable=print_context,  # какую python команду выполнить в этом таске
        op_kwargs={"line": "StartML is a starter course for ambitious people"}
    )
    
    task_false = PythonOperator(
        task_id="not_startml_desc", 
        python_callable=print_context,  # какую python команду выполнить в этом таске
        op_kwargs={"line": "Not a startML course, sorry"}
    )
    
    task_finish = DummyOperator(
        task_id='after_branching',
    )

    task_start >> task_branch >> [task_true, task_false] >> task_finish

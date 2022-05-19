from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
from airflow.models import Variable


def get_task_id():
    is_startml = Variable.get('is_startml')
    if is_startml == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def print_1():
    print('StartML is a starter course for ambitious people')


def print_2():
    print('Not a startML course, sorry')


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


with DAG(
    'lesson11_s-kosouhov_task_12',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 10),
    catchup=False,
    tags=['task_12'],
) as dag:

    task_0 = DummyOperator(
        task_id = f"start"
    )

    task_1 = BranchPythonOperator(
        task_id = f"task_print_var",
        python_callable=get_task_id,
    )

    task_2_1 = PythonOperator(
        task_id = f"startml_desc",
        python_callable=print_1,
    )

    task_2_2 = PythonOperator(
        task_id = f"not_startml_desc",
        python_callable=print_2,
    )

    task_end = DummyOperator(
        task_id = f"finish"
    )

    task_0 >> task_1 >> [task_2_1, task_2_2] >> task_end
    
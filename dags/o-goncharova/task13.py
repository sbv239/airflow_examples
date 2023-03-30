from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def startml_desc():
    print('StartML is a starter course for ambitious people')

def not_start_desc():
    print('Not a startML course, sorry')


with DAG(
    'dag_task_13_o-goncharova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    start_date = datetime(2023, 3, 30)
) as dag:
    t1 = DummyOperator(
        task_id='before_branching',
    )
    t2 = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = get_variable,
    )
    t3 = PythonOperator(
        task_id = 'startml_desc',
        python_callable  = startml_desc,
    )
    t4 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = not_start_desc,
    )
    t5 = DummyOperator(
        task_id = 'after_branching',
    )
t1 >> t2 >> [t3, t4] >> t5

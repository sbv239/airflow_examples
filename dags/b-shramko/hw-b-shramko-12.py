from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator


def choose_branch():
    from airflow.models import Variable

    res = Variable.get("is_startml")
    return 'startml_desc' if res == 'True' else 'not_startml_desc'


def print_t2():
    print("StartML is a starter course for ambitious people")


def print_t3():
    print("Not a startML course, sorry")


with DAG('hw-b-shramko-11',
         default_args={
             'depends_on_past': False,
             'email': ['airflow@example.com'],
             'email_on_failure': False,
             'email_on_retry': False,
             'retries': 1,
             'retry_delay': timedelta(minutes=5),
         },
         start_date=datetime(2022, 1, 1),
         catchup=False
         ) as dag:
    t1 = BranchPythonOperator(
        task_id='where_to_go',
        python_callable=choose_branch
    )
    t2 = PythonOperator(task_id="startml_desc", python_callable=print_t2)
    t3 = PythonOperator(task_id="not_startml_desc", python_callable=print_t3)

    t1 >> [t2, t3]

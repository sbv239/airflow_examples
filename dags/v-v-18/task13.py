from airflow import DAG
from airflow.operators.python import task, PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable


with DAG(
    'dag_task13',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date = datetime(2023, 3, 23)
) as dag:
    
    t1 = DummyOperator(task_id="start")

    
    def check_the_way(*kwargs):
        my_value = Variable.get("is_startml")
        xcom_value = my_value.xcom_pull(task_ids='start_task')
        if xcom_value == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"
    
    t2 = BranchPythonOperator(
    task_id='branch_task',
    python_callable=check_the_way,
    )

    t2 = PythonOperator(
        task_id = "print_var",
        python_callable = print_var
    )

    t3 = BashOperator(
        task_id = "startml_desc",
        bash_command = "echo StartML is a starter course for ambitious people"
    )

    t4 = BashOperator(
        task_id="not_startml_desc",
        bash_command = "echo Not a startML course, sorry"
        )
    t5 = DummyOperator(task_id="Finish")

    t1 >> t2 >> [t3, t4] >> t5
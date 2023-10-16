from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta


with DAG(
'hw_e-mihalev_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple DAG',
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['example']
) as dag:
    def branch_choice():
        if Variable.get("is_startml") is "True":
            return 'startml_desc'
        else:
            return 'not_startml_desc'


    def left_task():
        print("StartML is a starter course for ambitious people")


    def right_task():
        print("Not a startML course, sorry")


    t_first = DummyOperator(
        task_id='before_branching',
        dag=dag
    )

    branching = BranchPythonOperator(
        task_id='determine_course',
        python_callable=branch_choice,
        dag=dag
    )

    t_left = PythonOperator(
        task_id="startml_desc",
        python_callable=left_task,
        dag=dag
    )

    t_right = PythonOperator(
        task_id="not_startml_desc",
        python_callable=right_task,
        dag=dag
    )

    t_last = DummyOperator(
        task_id='after_branching',
        dag=dag
    )

    t_first >> branching >> [t_left, t_right] >> t_last

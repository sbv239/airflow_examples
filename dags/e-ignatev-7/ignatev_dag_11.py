from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'ignatev_dag_11',
    default_args=default_args,
    start_date=datetime(2022, 4, 15),
    max_active_runs=1,
    schedule_interval=timedelta(days = 1),
) as dag:

    def branching():
        from airflow.models import Variable

        is_startml = Variable.get('is_startml')
        if is_startml == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    def is_startml():
        print('StartML is a starter course for ambitious people')

    def not_startml():
        print('Not a startML course, sorry')

    t1 = DummyOperator(
        task_id='before_branching',
    )
    
    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=branching,
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=is_startml,
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml,
    )

    t5 = DummyOperator(
        task_id='after_branching',
    )

    t1 >> t2 >> [t3, t4] >> t5
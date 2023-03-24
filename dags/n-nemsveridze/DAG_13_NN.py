from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable


with DAG(
        'task13NN',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='set Variable',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 21),
        catchup=False,
        tags=['NNtask13'],
) as dag:
    def branching():
        is_startml = Variable.get('is_startml')

        if is_startml == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'


    def print_startml_desc():
        print("StartML is a starter course for ambitious people")


    def print_not_startml_desc():
        print("Not a startML course, sorry")


    task1 = BranchPythonOperator(task_id='printVar',
                           python_callable=branching)

    task2= PythonOperator(task_id='startml_desc',
                          python_callable=print_startml_desc)

    task3= PythonOperator(task_id='not_startml_desc',
                          python_callable=print_not_startml_desc)

    before = DummyOperator(task_id = 'before')
    after = DummyOperator(task_id = 'after')
    before>>task1>>[task2,task3]>> after
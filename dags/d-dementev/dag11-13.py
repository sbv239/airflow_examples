from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    'unit11-13-dementev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },

    description='DAG unit 11-13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    def choose():
        if Variable.get("is_startml") == 'True':
            return "startml_desc"
        else:
            return "not_startml_desc"

    choose = BranchPythonOperator(
        task_id='choose_oper',
        python_callable=choose,
    )
    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=lambda: 'StartML is a starter course for ambitious people'
    )
    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=lambda: 'Not a startML course, sorry'
    )
    dummy = DummyOperator(
        task_id='Dummy'
    )
    dummy2 = DummyOperator(
        task_id='Dummy2'
    )
    dummy >> choose >> [t1, t2] >> dummy2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable

def branch():
    value = Variable.get('is_startml')
    if value == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def startml_desc():
    print('StartML is a starter course for ambitious people')

def not_startml_desc():
    print('Not a startML course, sorry')
    

with DAG(
    'hw_al-pivovarov_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now()
) as dag:
    branch = BranchPythonOperator(task_id='branch', python_callable=branch)
    t1 = PythonOperator(task_id='startml_desc', python_callable=startml_desc)
    t2 = PythonOperator(task_id='not_startml_desc', python_callable=not_startml_desc)
    branch >> [t1, t2]
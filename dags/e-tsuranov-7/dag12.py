from datetime import datetime, timedelta #12
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'hw_12_e-tsuranov-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 15),
    catchup=False,
    tags=['tsuranov'],
) as dag:

    t1 = DummyOperator(task_id='dummy_begin')

    t2 = BranchPythonOperator(task_id='choise', python_callable=lambda: 'startml_desc' if (Variable.get('is_startml') == True) else 'not_startml_desc')

    t3 = PythonOperator(task_id='startml_desc', python_callable=lambda: print('StartML is a starter course for ambitious people'))
    
    t4 = PythonOperator(task_id='not_startml_desc', python_callable=lambda: print('Not a startML course, sorry'))
    
    t5 = DummyOperator(task_id='dummy_end')
    
    t1 >> t2 >> [t3, t4] >> t5
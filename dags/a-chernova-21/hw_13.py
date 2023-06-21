from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

def cond_var():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return "startml_desc"
    else:
        return "not_startml_desc"

def startml_desc():
    print('StartML is a starter course for ambitious people')
    
def not_startml_desc():
    print('Not a startML course, sorry')
    

with DAG(
    'hw_a-chernova-21_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='A new dag for db',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 20),
    catchup=False,
    tags=['xcom_dt']
) as dag:
    
    t0 = DummyOperator(
        task_id = 'before_branching')
    
    t1 = BranchPythonOperator(
        task_id = 'determine_cource',
        python_callable=cond_var)
    
    t2 = PythonOperator(
        task_id = 'startml_desc',
        python_callable=startml_desc)
    
    t3 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable=not_startml_desc)
    
    t4 = DummyOperator(
        task_id = 'after_branching')
    
t0 >> t1 >> [t2, t3] >> t4
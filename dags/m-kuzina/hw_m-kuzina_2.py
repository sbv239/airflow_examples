from airflow import DAG 
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator




def print_ds(ds, **kwargs):
    print (ds)
    print ('Thats all')
           
           
with DAG(
    'hw_m-kuzina_2', 
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    shedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 19),
    catchup=False 
) as dag:
    
    
    t1=BashOperator(
        task_id='call pwd',
        bash_command='pwd'
    )
    
    t2=PythonOperator(
        task_id = 'print ds',
        python_callable = print_ds
    )
    
    t1 >> t2
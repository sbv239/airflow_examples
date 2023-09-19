from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG


from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
 

with DAG(
    'hw_ta-korobitsyna_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
        },
    description='hw_13_ta-korobitsyna',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 19),
    catchup=False,
    tags=['hw_ta-korobitsyna_13'],
) as dag:
 
     

    def choose_branch():
        if Variable.get('is_startml') == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    branching = BranchPythonOperator(
        task_id='choose_branch_13',
        python_callable=choose_branch,
        
        )

    def print_var_tr():
        print('StartML is a starter course for ambitious people')

    def print_var_fl():
            print('Not a startML course, sorry')

         
    t1 = PythonOperator(
        task_id="startml_desc",  # нужен task_id, как и всем операторам
        python_callable=print_var_tr, 
        )
    
    t2 = PythonOperator(
        task_id="not_startml_desc",  # нужен task_id, как и всем операторам
        python_callable=print_var_fl, 
        )
    
    branching >> [t1 , t2]
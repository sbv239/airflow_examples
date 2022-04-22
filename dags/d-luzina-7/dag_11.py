from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator



with DAG(
    'dag_11_d-luzina-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  
    },
    description='dag 11 lesson 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    # теги, способ помечать даги
    tags=['dag_11_d-luzina-7'],
) as dag:
    
        
    def get_var():
        from airflow.models import Variable
        print(Variable.get("is_startml"))
        return 'None'

    t1 = PythonOperator(
        task_id='print_variable',  
        python_callable=get_var
    )
    
    t1
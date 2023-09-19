from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG


from airflow.operators.python import PythonOperator
from airflow.models import Variable
 

with DAG(
    'hw_ta-korobitsyna_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
        },
    description='hw_12_ta-korobitsyna',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 19),
    catchup=False,
    tags=['hw_ta-korobitsyna_12'],
) as dag:

     
    def print_var():
        is_startml = Variable.get("is_startml")
        print(is_startml) 
            
    t2 = PythonOperator(
        task_id="task_is_strml_12",  # нужен task_id, как и всем операторам
        python_callable=print_var, 
        )
    

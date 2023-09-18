from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def push_xcom(ti):
    ti.xcom_push(key='sample_xcom_key',
                 value='xcom test')
    
def pull_data(ti):
    answer = ti.xcom_pull(task_ids='task_pull',
                          key='sample_xcom_key')
    print(answer)

default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
        }

with DAG(
    'hw_ta-korobitsyna_9',
    default_args = default_args,
    description='hw_9_ta-korobitsyna',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 18),
    catchup=False,
    tags=['hw_ta-korobitsyna_9'],
) as dag:

    t1 = PythonOperator(
        task_id='task_pull',  
        python_callable=push_xcom,  
        op_kwargs={'xcom_data': 'xcom test'}
    )


    t2 = PythonOperator(
        task_id='task_les_11_9',  
        python_callable=pull_data, 
    )
    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
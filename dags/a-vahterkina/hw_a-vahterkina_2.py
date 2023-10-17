from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_a-vahterkina_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 16),
    catchup=False
) as dag:


    t = BashOperator(
        task_id="print_the_context",
        bash_command="pwd",
    )
    
    t1 = PythonOperator(
        task_id='print_the_context',  # нужен task_id, как и всем операторам
        python_callableprint_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

t >> t1

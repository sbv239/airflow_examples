from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from airflow.models import Variable

with DAG(
    'hw_12_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['hw_al-savelev']
) as dag:

    def print_variable():
        result = Variable.get('is_startml')
        print(result)
  
    t1 = PythonOperator(
        task_id='hw_12_al-savelev',
        python_callable=print_variable
        )

    t1

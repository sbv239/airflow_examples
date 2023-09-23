from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def print_value():
    print(Variable.get('is_startml'))

with DAG(
    'hw_al-pivovarov_12',
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
    t1 = PythonOperator(task_id='print', python_callable=print_value)
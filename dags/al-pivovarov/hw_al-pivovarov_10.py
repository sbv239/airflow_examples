from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def return_str():
    return 'Airflow tracks everything'

def get_str(ti):
    result = ti.xcom_pull(key='return_value', task_ids='push')
    print(result)

with DAG(
    'hw_al-pivovarov_10',
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
    t1 = PythonOperator(task_id='push', python_callable=return_str)
    t2 = PythonOperator(task_id='pull', python_callable=get_str)
    t1 >> t2
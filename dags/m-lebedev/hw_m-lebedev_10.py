from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def return_string():
    return "Airflow tracks everything"
    
def use_xcom(ti):
    test = ti.xcom_pull(
        key='return_value',
        task_ids='print_string'
    )
    print(test)

with DAG(
    'hw_m-lebedev_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework: 10, login: m-lebedev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 24),
    catchup=False,
    tags=['m-lebedev'],
) as dag:

    t1 =  PythonOperator(
        task_id = 'print_string',
        python_callable=return_string,
    )
    
    t2 =  PythonOperator(
        task_id= 'xcom_pull',
        python_callable=use_xcom,
    )
    
    t1 >> t2
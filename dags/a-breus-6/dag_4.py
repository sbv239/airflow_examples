from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'task_8_breus',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    'retry_delay': timedelta(minutes=5), 
    },

    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

    def push_to_xcom(ti):

        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test')

    def get_from_xcom(ti):

        sample_xcom = ti.xcom_pull(
            key='sample_xcom_key')
        
        print(sample_xcom)

    t1 = PythonOperator(
        task_id = 'xcom_push',
        python_callable=push_to_xcom
    )

    t2 = PythonOperator(
        task_id = 'xcom_pull',
        python_callable=get_from_xcom
    )

    t1 >> t2
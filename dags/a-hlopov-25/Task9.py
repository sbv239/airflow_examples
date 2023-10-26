from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def xcom_put(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )
def xcom_get(ti):
    read_value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='xcom_get',
    )
    print('Recieved value of sample_xcom_key from xcom: ', read_value)

with DAG(
        'hw_9_a-hlopov-25',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  
        },
        description='Task 9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 25),
        catchup=False,

        tags=['Task_9'],
) as dag:
    t1 = PythonOperator(
        task_id='xcom_set',
        python_callable=xcom_put
    )

    t2 = PythonOperator(
        task_id='xcom_get',
        python_callable=xcom_get
    )
    t1 >> t2
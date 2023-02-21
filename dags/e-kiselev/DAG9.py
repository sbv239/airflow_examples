from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def xcom_push(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )
def xcom_pull(ti):
    xcom = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='xcom_push',
    )
    print(xcom)

with DAG(
    'hw_9_e-kiselev',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Ex. 9.',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 16),
    catchup=False,
    tags=['hw_9_e-kiselev'],
) as dag:

    t1 = PythonOperator(
        task_id= "xcom_push",
        python_callable=xcom_push,
    )
    t2 = PythonOperator(
        task_id="xcom_pull",
        python_callable=xcom_pull,
    )
    t1 >> t2
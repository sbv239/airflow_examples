from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
'hw_e-mihalev_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple DAG',
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['example']
) as dag:
    def xcom_task1(ti):
        return "Airflow tracks everything"
    def xcom_task2(ti):
        print(ti.xcom_pull(key="return_value", task_ids="task_push"))
    task_1 = PythonOperator(task_id='task_push', python_callable=xcom_task1)
    task_2 = PythonOperator(task_id='task_pull', python_callable=xcom_task2)
    task_1 >> task_2
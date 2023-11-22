from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'hw_10_e-shajapin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
) as dag:

    def xcom_push():
        return "Airflow tracks everything"

    def pull_xcom(ti):
        result = ti.xcom_pull(key='return_value', task_ids='xcom_push')
        print(result)


    t1 = PythonOperator(task_id="xcom_push", python_callable=xcom_push)

    t2 = PythonOperator(task_id="pull_xcom", python_callable=pull_xcom)

    t1 >> t2

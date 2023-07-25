from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

xcom_test = 'xcom test'

def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value=xcom_test
    )


def pull_data(ti):
    answer = ti.xcom_pull(
        task_ids='push_xcom',
        key='sample_xcom_key'
    )
    print(answer)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_p-pertsov-36_9',
        start_date=datetime(2023, 6, 1),
        schedule_interval=timedelta(minutes=5),
        max_active_runs=2,
        default_args=default_args,
        catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='push_xcom',
        dag=dag,
        python_callable=push_xcom,
    )
    t2 = PythonOperator(
        task_id='pull_data',
        dag=dag,
        python_callable=pull_data
    )
    t1 >> t2


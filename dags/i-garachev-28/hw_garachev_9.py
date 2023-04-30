from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    'tutorial',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_garachev_9_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_9'],
) as dag:


    def put_data(ti):
        ti.xcom_push(key="sample_xcom_key", value="xcom test")

    def get_data(ti):
        print(ti.xcom_pull(key="sample_xcom_key", task_ids="hw_9_garachev_put_data_task"))

    t1 = PythonOperator(
        task_id='hw_9_garachev_put_data_task',
        python_callable=put_data
    )

    t2 = PythonOperator(
        task_id='hw_9_garachev_get_data_task',
        python_callable=get_data
    )

    t1 >> t2
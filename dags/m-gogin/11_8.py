from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


with DAG(
        'hw_8_m-gogin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_8_m-gogin',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['hw_6'],
) as dag:

    def add_key(x):
        x.xcom_push(
            key="sample_xcom_key",
            value="xcom test"
        )

    def print_key(x):
        x.xcom_print = x.xcom_pull(
            key='sample_xcom_key',
            task_id='push_xcom'
        )
        print(x.xcom_print)


    t1 = PythonOperator(
        task_id="add_key",
        python_callable=add_key
    )

    t2 = PythonOperator(
        task_id="print_key",
        python_callable=print_key
    )

    t1 >> t2


from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator


with DAG(
        'v-susanin_task_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='v-susanin_DAG_task_10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 26),
        catchup=False,
        tags=['DAG_task_10'],
) as dag:
    def push_xcom_func(ti):
        return "Airflow tracks everything"


    def pull_xcom_func(ti):
        value_read = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(value_read)


    first = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom_func,
    )

    second = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom_func,
    )

    first >> second

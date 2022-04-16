from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
        'hw_7_m-valishevskij-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 14),
        catchup=False,
        tags=['valishevskij']
) as dag:
    def xcom_push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def xcom_pull_and_print(ti):
        value = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='hw_1_m-valishevskij-7_1'
        )
        print(value)

    t1 = PythonOperator(
        task_id='hw_1_m-valishevskij-7_2',
        python_callable=xcom_push
    )

    t2 = PythonOperator(
        task_id='hw_1_m-valishevskij-7_2',
        python_callable=xcom_pull_and_print
    )


    t1 >> t2

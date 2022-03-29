from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'hw_8_v-kovalev-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_8'],
) as dag:

    def push_xcom(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def pull_xcom(ti):
        testing_pull = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        )
        print(testing_pull)

    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )

    t1 >> t2

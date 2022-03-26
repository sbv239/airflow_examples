from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
        'hw_9_v-kovalev-6',
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
        tags=['hw_9'],
) as dag:

    def return_string():
        return "Airflow tracks everything"


    def pull_xcom(ti):
        testing_pull = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(testing_pull)


    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=return_string,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )

    t1 >> t2

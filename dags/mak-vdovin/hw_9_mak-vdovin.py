from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'explicit_xcom',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "explict xcom"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_9'],
) as dag:

    def xcom_push(ti):
        ti.xcom_push(
            key="sample_xcom_key",
            value='xcom test'
        )

    def xcom_pull(ti):
        value = ti.xcom_pull(
            key="sample_xcom_key",
            task_ids='xcom_push'
        )
        print(value)
        return value

    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=xcom_push,
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull,
    )

    t1 >> t2

    if __name__ == "__main__":
        dag.test()
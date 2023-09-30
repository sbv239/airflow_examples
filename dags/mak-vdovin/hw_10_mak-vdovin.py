from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'implicit_xcom',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "implict xcom"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_10'],
) as dag:

    def xcom_push_():
        return 'Airflow tracks everything'

    def xcom_pull(ti):
        value = ti.xcom_pull(
            key="return_value",
            task_ids='xcom_push_'
        )
        print(value)
        return value

    t1 = PythonOperator(
        task_id='xcom_push_',
        python_callable=xcom_push_,
        do_xcom_push=True # True is default
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull,
    )

    t1 >> t2

    if __name__ == "__main__":
        dag.test()
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def set_xcom_func():
    return 'Airflow tracks everything'


def get_xcom_func(ti):
    # value_read = ti.xcom_pull(key='sample_xcom_key')
    value_read = ti.xcom_pull(
        key='return_value',
        task_ids='set_xcom'
    )
    print(value_read)


with DAG(
        'dag_9_m-tsaj',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A bit more of XCom',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='set_xcom',
        python_callable=set_xcom_func,
    )

    t2 = PythonOperator(
        task_id='get_xcom',
        python_callable=get_xcom_func,
    )

    t1 >> t2

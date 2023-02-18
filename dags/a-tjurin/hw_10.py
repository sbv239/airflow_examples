from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def x_com_push(ti):
    ti.xcom_push(
        key='to_return_xcom_key',
    )
    return "Airflow tracks everything"


def x_com_pull(ti):
    data_read_push = ti.xcom_pull(
        key='return_value',
        task_ids='push_data_out',
    )
    print('Test xcom: ', data_read_push)


with DAG(
        'hw_10_a-tjurin',  
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
        },

        description='Task 10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 18),
        catchup=False,

        tags=['Task_10'],
) as dag:
    t1 = PythonOperator(
        task_id='push_data_out',
        python_callable=x_com_push
    )

    t2 = PythonOperator(
        task_id='pull_data_in',
        python_callable=x_com_pull
    )

    t1 >> t2

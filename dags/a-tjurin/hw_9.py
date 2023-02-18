from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_testing_increase(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def get_testing_data(ti):
    data_read_push = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_data_out',
    )
    print('Test xcom: ', data_read_push)



with DAG(
        'hw_9_a-tjurin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5), 
        },

        description='Task 9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 18),
        catchup=False,

        tags=['Task_9'],
) as dag:

    t1 = PythonOperator(
        task_id='push_data_out',
        python_callable=get_testing_increase
    )

    t2 = PythonOperator(
        task_id='pull_data_in',
        python_callable=get_testing_data
    )

    t1 >> t2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def return_string():
    return 'Airflow tracks everything'


def get_string_value(ti):
    get_value = ti.xcom_pull(
        key='return_value',
        task_ids='get_return'
    )
    print(get_value)


with DAG(
        'n-anufrieva_task10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='n-anufrieva_task10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['n-anufrieva_task10'],
) as dag:

    task_1 = PythonOperator(
        task_id='string',
        python_callable=return_string,
    )
    task_2 = PythonOperator(
        task_id='get_string',
        python_callable=get_string_value,
    )

    task_1 >> task_2

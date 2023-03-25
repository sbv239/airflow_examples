from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def get_variable():
    from airflow.models import Variable

    is_startml = Variable.get("is_startml")  # необходимо передать имя, заданное при создании Variable
    print(is_startml)
    return is_startml


def xcom_pull(ti):
    testing_increases = ti.xcom_pull(
        key='return_value',
        task_ids='get_variable'
    )
    # print(testing_increases)


with DAG(
        'e-kim-18_task_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A DAG for task 02',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 20),
        catchup=False,
        tags=['e-kim-18-tag'],
) as dag:
    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable,
    )

    t1

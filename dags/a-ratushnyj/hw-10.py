"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator


def put_some_impl():
    return f"Airflow tracks everything"

def get_some_impl(ti):
    get_some_date = ti.xcom_pull(
        key='return_value',
        task_ids='get_some_data_from_x_com_impl'
    )
    print(get_some_date)

with DAG(
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},

        start_date=datetime(2023, 9, 22),
        dag_id="hw_10_a-ratushnyj",
        schedule_interval=timedelta(days=1),
        tags=['hw-10'],

) as dag:
    opr_put_data_impl = PythonOperator(
        task_id = 'get_some_data_from_x_com_impl',
        python_callable=put_some_impl
    )
    opr_get_data_impl = PythonOperator(
        task_id = 'get_test2',
        python_callable=get_some_impl
    )

    opr_put_data_impl >> opr_get_data_impl

"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator


def put_some(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )

def get_some(ti):
    get_some_date = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='get_some_data_from_x_com'
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

        start_date=datetime(2023, 9, 21),
        dag_id="hw_9_a-ratushnyj",
        schedule_interval=timedelta(days=1),
        tags=['hw-9'],

) as dag:
    opr_put_data = PythonOperator(
        task_id = 'get_some_data_from_x_com',
        python_callable=put_some
    )
    opr_get_data = PythonOperator(
        task_id = 'get_test',
        python_callable=get_some
    )

    opr_put_data >> opr_get_data

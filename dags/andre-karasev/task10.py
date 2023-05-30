from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def get_string():
    return "Airflow tracks everything"


def pull_xcom(ti):
    result = ti.xcom_pull(
        key="return_value",
        task_ids="xcom_push"
    )
    return result


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'andre-karasev_hw_9',
        default_args=default_args,
        description='hw_9_',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 9),
        catchup=False,
        tags=['andre-karasev_hw_9']
) as dag:
        t1 = PythonOperator(
                task_id="xcom_push",
                python_callable=get_string
        )

        t2 = PythonOperator(
                task_id="xcom_pull",
                python_callable=pull_xcom
        )

        t1 >> t2
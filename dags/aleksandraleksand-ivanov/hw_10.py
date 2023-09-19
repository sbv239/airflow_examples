from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        dag_id="hw_aleksandraleksand-ivanov_10",
        default_args=default_args,
        start_date=datetime(2023, 9, 18),
        schedule_interval=timedelta(days=1)
) as dag:
    def return_string():
        return "Airflow tracks everything"

    def pulL_string(ti):
        print(ti.xcom_pull(task_ids="return_str", key="return_value"))

    return_str = PythonOperator(
        task_id='return_str',
        python_callable=return_string
    )

    pull = PythonOperator(
        task_id="pull",
        python_callable=pulL_string
    )

    return_str >> pull
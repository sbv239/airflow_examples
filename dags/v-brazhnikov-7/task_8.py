from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    "task_8",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": days_ago(2),
    },
    catchup=False,
) as dag:

    def place_to_xcom(ti, **kwargs):
        ti.xcom_push(key="sample_xcom_key", value="xcom test")

    t1 = PythonOperator(
        task_id=f"xcom_pusher",
        python_callable=place_to_xcom,
        dag=dag,
    )

    def print_from_xcom(ti, **kwargs):
        sample_value = ti.xcom_pull(key="sample_xcom_key", task_ids="xcom_pusher")
        print(sample_value)

    t2 = PythonOperator(
        task_id=f"xcom_puller",
        python_callable=print_from_xcom,
        dag=dag,
    )

    t1 >> t2

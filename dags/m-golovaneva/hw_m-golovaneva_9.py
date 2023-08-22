from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def pushes_v(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )
def pulls_v(ti):
    pulled_value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="try_xcom_push"
    )
    print(f"The value we get through xcom: {pulled_value}")


with DAG(
        "hw_m-golovaneva_task9",

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='trying to play around XCom',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,

        tags=['mariaSG']

) as dag:

    t1_push_value = PythonOperator(
        task_id="try_xcom_push",
        python_callable=pushes_v
    )

    t2_pull_value = PythonOperator(
        task_id="try_xcom_pull",
        python_callable=pulls_v
    )

    t1_push_value >> t2_pull_value

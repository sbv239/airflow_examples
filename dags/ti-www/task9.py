from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    "ti-www_task9",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="DAG_with_interaction_with_XCom",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 19),
    catchup=False,
    tags=["ti-www"],
) as dag:

    def push_data(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def get_data(ti):
        tst = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_data_to_XCom'
        )
        print(f"Warning! Check value: {tst}")

    send_data = PythonOperator(
        task_id="push_data_to_XCom",
        python_callable=push_data,
        
    )

    print_data = PythonOperator(
        task_id="get_data_from_XCom",
        python_callable=get_data,
    )

    send_data >> print_data
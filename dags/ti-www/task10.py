from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    "ti-www_task10",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="DAG_with_interaction_with_XCom.Check_implicit_return",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 19),
    catchup=False,
    tags=["ti-www"],
) as dag:

    def return_data(ti):
        print("Now it happens magic!")
        return "Airflow tracks everything"

    def get_data(ti):
        tst = ti.xcom_pull(
            key='return_value',
            task_ids='check_implicit_pushing_to_XCom'
        )
        print(f"Warning! Check value: {tst}")

    send_data = PythonOperator(
        task_id="check_implicit_pushing_to_XCom",
        python_callable=return_data,
        
    )

    print_data = PythonOperator(
        task_id="get_data_from_XCom",
        python_callable=get_data,
    )

    send_data >> print_data
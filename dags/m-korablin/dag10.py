from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


with DAG(
    'hw_10_m-korablin',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Xcom',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['VanDarkholme'],
) as dag:

    def push_Xcom(ti):
        return "Airflow tracks everything"


    def pull_Xcom(ti):
        ans = ti.xcom_pull(
            key="return_value",
            task_ids="pushing_Xcom"
        )
        return ans


    tpushX = PythonOperator(
        task_id='pushing_Xcom',
        python_callable=push_Xcom
    )
    tpullX = PythonOperator(
        task_id='pulling_Xcom',
        python_callable=pull_Xcom
    )

    tpushX >> tpullX
    
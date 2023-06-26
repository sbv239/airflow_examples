from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def set_testing_data():
    """
    Первый PythonOperator должен вызвать функцию, возвращающую строку "Airflow tracks everything".
    """
    return "Airflow tracks everything"

def get_testing_data(ti):
    """
    Второй PythonOperator должен получить эту строку через XCom.
    """
    result = ti.xcom_pull(
        key="return_value",
        task_ids='hw_n-chuviurova_1'
    )
    print(result)



with DAG(
        "hw_10_n-chuviurova",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="XCom2",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 15),
        catchup=False,
        tags=["task_10"],
) as dag:

    t1 = PythonOperator(
        task_id="hw_n-chuviurova_1",
        python_callable=set_testing_data,
    )
    t2 = PythonOperator(
        task_id="hw_n-chuviurova_2",
        python_callable=get_testing_data,
    )

    t1 >> t2

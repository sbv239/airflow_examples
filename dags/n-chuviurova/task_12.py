from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable


def get_variables():
    """
    Этот оператор должен печатать значение Variable с названием is_startml.
    """

    is_startml = Variable.get("is_startml")
    print(is_startml)


with DAG(
        "hw_12_n-chuviurova",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="Variables",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 15),
        catchup=False,
        tags=["task_12"],
) as dag:

    t1 = PythonOperator(
        task_id="hw_n-chuviurova_1",
        python_callable=get_variables,
    )

    t1

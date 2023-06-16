from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_context(ts, run_id, **kwargs):
    print(ts)
    print(run_id)
    print(f"task number is: ${kwargs['task_number']}")


with DAG(
        "hw_7_n-chuviurova",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="kwargs",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 15),
        catchup=False,
        tags=["task_07"],
) as dag:
    for j in range(10, 30):
        t2 = PythonOperator(
            task_id="hw_n-chuviurova_" + str(j),
            python_callable=print_context,
            op_kwargs={"task_number": j},
        )

    t2

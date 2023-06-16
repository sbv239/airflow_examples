from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        "hw_6_n-chuviurova",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        description="env",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 15),
        catchup=False,
        tags=["task_06"],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id="hw_n-chuviurova_" + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
        )

    t1

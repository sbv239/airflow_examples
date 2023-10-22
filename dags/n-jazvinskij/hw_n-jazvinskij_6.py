from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'hw_11_ex_6-n-jazvinskij',
    default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    description = 'hw_11_ex_6',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 10, 21),
    catchup = False,
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id = 'Bash_task_' + str(i),
            bash_command = "echo $NUMBER",
            env = {'NUMBER': str(i)}
        )
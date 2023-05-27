import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = "Making DAG for 3rd task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 26),
    catchup = False,
    tags = ["hw_3_a-maslennikov"],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = f"print_echo_{i}",
            bash_command = f"echo {i}"
        )

    def print_task_num(task_number, **kwargs):
        print(kwargs)
        return f"task_number is: {task_number}"

    for i in range(20):
        t2 = PythonOperator(
            task_id = f"print_task_{i}",
            python_callable = print_task_num,
            op_kwargs = {"task_number": i}
        )

    t1 >> t2

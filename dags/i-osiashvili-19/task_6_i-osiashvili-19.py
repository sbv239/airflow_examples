from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from token import NUMBER


def task_number_is(task_number):
    print("task number is: {task_number}")


with DAG(
        'les_11_task_6_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 20),
        schedule_interval=timedelta(days=1),

) as dag:
    for i in range(10):
        bash_op_tasks = BashOperator(
            task_id="task_num_" + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i},
        )

from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime


def print_context(task_n: int, ts, run_id, **kwargs):
    print("task number is: {kwargs['task_number']}")
    print(ts)
    print(run_id)

templated_command = dedent(
        """
    {% for i in range({NUMBER}) %}
        echo "{{ ts }}"
    {% endfor %}
    echo "{{ run_id }}"
    """)
with DAG(
    "what_the_hw7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='fuck DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=3, day=22),
    catchup=False,
    tags=['hw_7'],
) as dag:
    for i in range(1,11):
        t1 = BashOperator(
        task_id='BO7' + str(i)',  # id, будет отображаться в интерфейсе
        bash_command= templated_command  # какую bash команду выполнить в этом таске
    )
    for task_number in range(1, 21):
        t2 = PythonOperator(
            task_id=f'print_task_{task_number}',
            python_callable=print_context,
            op_kwargs={'task_number': task_n}
    )
    t2.doc_md = dedent(
        """
    # Task with PythonOperator
    This **task** *printing* 10 consecutive numbers
    in the form `task number is: {task_number}`
        """
    )

    t1 >> t2
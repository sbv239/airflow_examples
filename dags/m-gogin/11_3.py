from textwrap import dedent
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_3_m-gogin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='11_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 20),
    catchup=False,
    tags=['example'],
) as dag:

    templaned_comand = dedent(
        """
    {% for i in range(10) %}
        f"echo {i}"
    {% endfor %}
    """
    )
    t1 = BashOperator(
        task_id='print',
        bash_command=templaned_comand,
    )

    def gen_func(task_number):
        return print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id='print2',
            python_callable=gen_func(),
            op_kwargs={'task_number': 3},
        )
    t1 >> t2





from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_s-palatov-23_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['123123'],
) as dag:
    for i in list(range(10)):
        t1 = BashOperator(
            task_id='print_echo_' + str(i),
            bash_command=f"echo {i}",
        )

    def funct_print_i(task_number):
        return f"task number is: {task_number}"

    for a in list(range(10,30,1)):
        t2 = PythonOperator(
            task_id='print_task_number_' + str(a),
            python_callable=funct_print_i,
            op_kwargs={'task_number': int(a)},
        )
    t1 >> t2
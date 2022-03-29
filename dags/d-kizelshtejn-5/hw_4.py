from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_tn(task_number):
    print(f"task_number is: {task_number}")


with DAG(
        'hw_4_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_4']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print' + str(i),
            bash_command=f"echo {i}",
        )

    t1.doc_md = dedent(
        """
        # Задаем первые 10 задач _**типа `BashOperator`**_
        *выполняем команду:* __`f"echo {i}"`__, где `i` - элемент цикла
        """
    )

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='print_tn' + str(i),
            python_callable=print_tn,
            op_kwargs={'task_number': i}
        )

    t2.doc_md = dedent(
        """
        # Задаем оставшиеся 20 задач _**типа `PythonOperator`**_
        *функция должна печатать: * __`"task number is: {task_number}"`__,
        где `task_number` - элемент цикла
        """
    )

    t1 >> t2
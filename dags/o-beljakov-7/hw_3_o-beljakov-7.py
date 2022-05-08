from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
        'hw_3_o-beljakov-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='hw_3_o-beljakov-7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 8),
        catchup=False,
        tags=['task_3']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f"echo {i}"
        )
        t1.doc_md = dedent(
            """\
            # Task Documentation
            Первые **10 задач** типа *BashOperator*,
            в них выполнена команда, использующая переменную цикла: "f"echo {i}")
            """
            ) #dedent - особенность Airflow, в него нужно оборачивать всю доку

    def get_task_number(task_number):
        print(f'task number is : {task_number}')
        return 'i printed'

    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number' + str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number': i}
        )
        t2.doc_md = dedent(
            """\
            # Task Documentation
            Оставшиеся **20 задач** типа *PythonOperator*, функция задействует переменную из цикла.
            Этого добились, передавая переменную через 'op_kwargs' и принимая ее на стороне функции.
            Функция печатает "task number: {task_number}", где task_number - номер задания из цикла.
            """)

    t1 >> t2

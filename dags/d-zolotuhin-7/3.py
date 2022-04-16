from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_3_d-zolotuhin-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task_03_d_zolotukhin',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 17),
    catchup=False,
    tags=['task_03_d_zolotukhin'],
) as dag:

    for i in range(1, 11):
        t1 = BashOperator(
            task_id='echo'+str(i),
            bash_command=f'echo {i}',
        )
        t1.doc_md = dedent(
        """
        # Task Documentation
        **Первые 10 задач типа BashOperator**,
        *в них выполнена команда,*
        `использующая переменную цикла: "f"echo {i}"`
        """
        )

    def python_task_number(task_number):
        print(f'task number is: {task_number}')
        return None

    for i in range(11, 31):
        t2 = PythonOperator(
            task_id=f'task_number'+str(i),
            python_callable=python_task_number,
            op_kwargs={'task_number': i}
        )
        t2.doc_md = dedent(
        """
        # Task Documentation
        **Оставшиеся 20 задач типа *PythonOperator*, функция задействует переменную из цикла.**
        *Этого добились, передавая переменную через op_kwargs и принимая ее на стороне функции.*
        `Функция печатает "task number is: {task_number}", где task_number - номер задания из цикла.`
        """
        )

    t1 >> t2
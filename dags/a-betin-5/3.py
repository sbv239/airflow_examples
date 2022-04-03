from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_2_a-betin-5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A second DAG',
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 2, 10),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['first'],
) as dag:
    def print_task(num):
        print(f"task number is: {num}")


    for i in range(30):
        if i < 10:
            bash_op = BashOperator(
                task_id='print_task'+str(i),  # id, будет отображаться в интерфейсе
                bash_command=f"echo {i}"  # какую bash команду выполнить в этом таске
            )
            bash_op.doc_md = dedent(
                """
            ## BashOperator
            В этом *таске* распечатывается подряд **10** чисел
            при помощи команды `echo {i}`
            """
            )
        else:
            python_op = PythonOperator(
                task_id='task_number_' + str(i),
                python_callable=print_task,
                op_kwargs={'num': i}
            )
            python_op.doc_md = dedent(
                """
            ## PythonOperator
            В этом *таске* распечатывается подряд **20** чисел начиная с *10*
            при помощи функции print_task внутри которой используется f-string `f"task number is: {num}"`
            """
            )

    bash_op >> python_op


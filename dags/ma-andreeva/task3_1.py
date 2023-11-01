
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'task_3_1_andreeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG for task 3_1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 30),
    catchup=False,
    tags=['task3_1','task_3_1','andreeva'],
) as dag:
    t1 = DummyOperator(task_id='start_dag')
    t2 = DummyOperator(task_id='wait_for_all_bash_operators')
    t3 = DummyOperator(task_id="finish_dag")

    for i in range(10):
        task_bash = BashOperator(
            task_id=f'print_bash_{i}',
            bash_command=f"echo {i}"
        )
        task_bash.doc_md = dedent(
            f"""\
                #### Task_bash {i} documentation
                Используя `Python`, можно достичь **больших** высот в _программировании_
                """
        )
        t1 >> task_bash >> t2
        # То же самое, что и
        # t2 << task_bash << t1

    def print_task_number (ts, run_id, **kwargs):
        print(kwargs)  # kwargs будет словарем, содержащим все переданные именованные аргументы
        print(f"task number is: {kwargs.get('task_number')}") # из него сможем достать task_number, например
        print(run_id) # run_id - это еще одна переменная, которую передает Airflow в функцию
        print(ts) # равно как и ts - time string (логическая дата и время запуска оператора)

    # Мой способ написания (по заданию нужен только task_number):
    #def print_task_number(task_number):
    #    print(f'task number is: {task_number}')

    for i in range (20):
        task_python = PythonOperator(
            task_id=f'print_python_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number':i},
        )
        task_python.doc_md = dedent(
            f"""\
                       #### Task_python {i} documentation
                       Используя `Python`, можно достичь **больших** высот в _программировании_
                       """
        )

        t2 >> task_python >> t3
        #То же самое, что и
        #t3 << task_python << t2





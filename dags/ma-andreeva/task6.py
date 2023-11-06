
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'task_6_andreeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG for task 6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 30),
    catchup=False,
    tags=['task6','task_6','andreeva'],
) as dag:
    t1 = DummyOperator(task_id='start_dag')
    t2 = DummyOperator(task_id='wait_for_all_bash_operators')
    t3 = DummyOperator(task_id="finish_dag")

    for i in range(10):
        task_bash = BashOperator(
            task_id=f'print_bash_{i}',
            bash_command='echo  "env_number: $NUMBER" ',
            env={"NUMBER": str(i)},
        )

        task_bash.doc_md = dedent(
            f"""\
                #### Task_bash {i} documentation
                Используя `Python`, можно достичь **больших** высот в _программировании_
                """
        )
        t1 >> task_bash >> t2

    def print_task_number (ts, run_id, **kwargs):
        print(kwargs.get('task_number'))
        #print (task_number) - если бы явно перечислили бы task_number  в аргументах функции:
        #def print_task_number (ts, run_id,task_number, **kwargs)
        print(run_id)
        print(ts)
        #Хотя нигде не объявляли run_id и ts, Airflow  знает эти переменные, Airflow сам передает
        # их в аргументы ф-ции print_task_number

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






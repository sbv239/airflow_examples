from airflow import DAG
from textwrap import dedent
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
with DAG(
        'cycle_command',
        default_args={
            'sepends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            },
        description='Executes output number for 10 times and "task number is: number" for 20 times',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021,1,1),
        catchup=False,
        ) as dag:

    #1 команда в цикле
    for i in range(10):
        t1 = BashOperator(
                task_id="num_" + str(i),
                bash_command=f'echo $NUMBER ',
                env={'NUMBER': str(i)},
                )
    t1.doc_md=dedent(
            """
            Bash command `echo {i}` prints the number of the task.
            #As command works in the cycle, it prints the number of the task *i* for **10 times**, each generation the nummber increases by 1.
            """
             )
    #2 команда в цикле и функция, которую на выполняет
    def task_numb(task_number):
        print(f'task number is: {number}')
    for i in range(20):
        t2 = PythonOperator(
                task_id='print_number_' + str(i),
                python_callable=task_numb,
                op_kwargs={'task_number': i},
                )
    t2.doc_md=dedent(
            """
            t2 calls the function `task_numb`, which was announced before.
            #Function **task_number** prints phrase *task number is* and then the numver of the task, which it gets as an argument.
            #t2 works in cycle and gives an argument to the callable function **task_numb** via *op_kwargs key* `op_kwargs={"task_number":i}`,
            where i is the number of cycle generation.
            """
            )
    t1>>t2

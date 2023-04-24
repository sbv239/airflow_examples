from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent


def print_cycle(ts, run_id, **kwargs):
    print('task_number: ', kwargs["task_number"])
    print('ts: ', ts)
    print('run_id: ', run_id)

with DAG(
    'd_kurm_hw6',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)},
    description='An exersize 1 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 17),
    catchup=False,
    ) as dag:


    
    templated_command = dedent(
        """
        {% for i in range(10) %}
            echo "{{ i }}"
        {% endfor %}
        """)
    """
    t1 = BashOperator(
        task_id='bash_cycle',
        depends_on_past=False,
        bash_command=templated_command,
    )
    """

    for i in range(10):
        t1 = BashOperator(
            task_id=f'bash_pwd_{i}',
            bash_command=f'echo {i}',
        )
        t1.doc_md = dedent(
        """
#### This is bash operator {i} task.
It contains `bash` _cycle defined_ tasks, every task **prints** iteration number
Bash tasks are defined **10** times.
        """)

    for i in range(20):
        p_task = PythonOperator(
            task_id='print_p_task_' + str(i),
            python_callable=print_cycle,
            op_kwargs={"task_number": i},
        )
        t1 >> p_task



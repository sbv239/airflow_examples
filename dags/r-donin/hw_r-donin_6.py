"""
Возьмите BashOperator из третьего задания (где создавали task через цикл)
    и подбросьте туда переменную окружения NUMBER, чье значение будет равно i из цикла. 
Распечатайте это значение в команде, указанной в операторе 
    (для этого используйте bash_command="echo $NUMBER").

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.models import Variable

with DAG(
    'hw_r-donin_6',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Exercise dag from step 6',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 9, 29),
    catchup = False,
    tags = ['startml', 'airflow', 'r-donin']
) as dag:
    
    for i in range(10):
        t_bash = BashOperator(
            task_id = 'step_6_b_task' + str(i),
            bash_command = f"echo $NUMBER",
            env={'NUMBER': i}
        )
        
    def print_task_num(task_number):
        print(f'task number is: {task_number}')
    for i in range(10,30):
        t_py = PythonOperator(
            task_id = 'step_6_p_task_' + str(i),
            python_callable = print_task_num,
            op_kwargs = {'task_number': i}
        )

    dag.doc_md = __doc__
    
    t_bash >> t_py
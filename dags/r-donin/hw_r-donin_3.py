"""
В прошлом примере все задачи в DAG были объявлены явно. 
Однако это не единственный способ задать DAG: можно использовать всю силу цикла for для объявления задач.
Создайте новый DAG и объявите в нем 30 задач. 
Первые 10 задач сделайте типа BashOperator и выполните в них произвольную команду, 
так или иначе использующую переменную цикла (например, можете указать f"echo {i}").
Оставшиеся 20 задач должны быть PythonOperator, при этом функция должна задействовать переменную из цикла. 
Вы можете добиться этого, если передадите переменную через op_kwargs и примете ее на стороне функции. 
Функция должна печатать "task number is: {task_number}", где task_number - номер задания из цикла. 

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
    'hw_r-donin_3',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Exercise dag from step 2',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2023, 9, 29),
    catchup = False,
    tags = ['startml', 'airflow', 'r-donin']
) as dag:
    
    for i in range(10):
        t_bash = BashOperator(
            task_id = 'step_3_task_' + str(i),
            bash_command = f"echo {i}"
        )
        
    def print_task_num(task_number):
        print(f'task number is: {task_number}')
    for i in range(10,30):
        t_py = PythonOperator(
            task_id = 'step_3_task_' + str(i),
            python_callable = print_task_num,
            op_kwargs = {'task_number': i}
        )

    dag.doc_md = __doc__
    
    t_bash >> t_py
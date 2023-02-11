"""
В прошлом примере все задачи в DAG были объявлены явно. 
Однако это не единственный способ задать DAG: можно использовать всю силу цикла for для объявления задач.

Создайте новый DAG и объявите в нем 30 задач. 
Первые 10 задач сделайте типа BashOperator 
и выполните в них произвольную команду, так или иначе использующую переменную цикла (например, можете указать f"echo {i}").

Оставшиеся 20 задач должны быть PythonOperator, 
при этом функция должна задействовать переменную из цикла. 
Вы можете добиться этого, если передадите переменную через op_kwargs и примете ее на стороне функции. 
Функция должна печатать "task number is: {task_number}", где task_number - номер задания из цикла. 
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from datetime import timedelta, datetime


with DAG (
    'k-d-t3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'description text',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023,1,1),
    catchup=False
) as dag:
    
    for i in range(10):
        bo = BashOperator(
            task_id = 'bo_t3_' + str(i),
            bash_command =  f"echo {i}"
        )

    def foo(task_number):
        return f"task number is: {task_number}"

    for i in range(20):
        po = PythonOperator(
            task_id = 'po_t3_' + str(i),
            python_callable = foo,
            op_kwargs= {'task_number': i}
        )

    bo >> po

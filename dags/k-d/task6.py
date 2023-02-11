"""
Возьмите BashOperator из второго задания (где создавали task через цикл) 
и подбросьте туда переменную окружения NUMBER, чье значение будет равно i из цикла. 
Распечатайте это значение в команде, указанной в операторе (для этого используйте bash_command="echo $NUMBER").


- **Как создать глобальную переменную?**
    
    Для её создания необходимо сделать так, чтобы  в `BashOperator` можно было её считать.
    
- **Используя какой аргумент можно считать переменную?**
    
    В аргументе `bash_command` через знак $ можно считывать переменные (особенности bash), 
    а далее сохранить полученную переменную с использованием аргумента `env`.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from textwrap import dedent


with DAG (
    'k-d',
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
        NUMBER = "{{ i }}"
        bo = BashOperator(
            task_id = 'bo_t3_' + str(i),
            env={"NUMBER": i},  # задает переменные окружения
            bash_command =  "echo $NUMBER",
        )



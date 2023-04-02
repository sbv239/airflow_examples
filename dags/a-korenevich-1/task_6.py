"""
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
Here is example of code: `bash -c pwd`
Here is example of cursive: *some text with cursive*
Here is example of bold: **some bold text**
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_6_a-korenevich-1',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    # Описание DAG (не тасок, а самого DAG)
    description='Task #6',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 4, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['a-korenevich-1'],
) as dag:

    dag.doc_md = dedent(__doc__)

    for i in range(10):
        # Каждый таск будет выводить его номер
        task = BashOperator(
            task_id='print_bash_env_task_id_' + str(i),  # id, будет отображаться в интерфейсе
            bash_command="echo $NUMBER",  # какую bash команду выполнить в этом таске
            dag=dag,
            env={'NUMBER': i}
        )

        task.doc_md = dedent(__doc__)
    
    def print_task_id(task_number):
        """Вывести task_number"""
        print(f'task number is: {task_number}')

    for i in range(10, 30):
        # Каждый таск будет выводить его номер
        task = PythonOperator(
            task_id='print_python_task_id_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_task_id,
            # передаем в аргумент с названием task_number номер таски i
            op_kwargs={'task_number': str(i)},
        )

        task.doc_md = dedent(__doc__)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent


with DAG(
    # имя дага, которое отразиться на сервере airflow
    'DAG_2_oshurek',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание самого дага
    description='Задание1. Напишите DAG, который будет содержать BashOperator и PythonOperator.'
                ' В функции PythonOperator примите аргумент ds и распечатайте его.',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example_2_oshurek'],
) as dag:
    
    def print_i(task_number):
        return f'task number is: {task_number}'
    
    for i in range(10):
        task_bash = BashOperator(
            task_id = 'task_bash_' + str(i),
            bash_command = f'echo {i}',
            )
    task_bash.doc_md = dedent(
        '''\
        # Абзац через решетку
        **полужирный**
        _курсив_
        'code'
        ''')
            
    for i in range(20):
        task_py = PythonOperator(
            task_id = 'print_task_number_' + str(i),
            python_callable = print_i,
            op_kwargs = {'task_number': i},
            )
    dag.doc_md = __doc__
    dag.doc_md = ''' # Абзац через решетку **полужирный** _курсив_ 'code'  '''
    templated_command = dedent(
        '''
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
        ''')
    
            
    task_bash >> task_py
    

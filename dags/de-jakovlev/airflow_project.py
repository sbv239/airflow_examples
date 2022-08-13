from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from airflow import DAG


def print_context(i):
    """Пример PythonOperator"""
    # Через синтаксис **kwargs можно получить словарь
    # с настройками Airflow. Значения оттуда могут пригодиться.
    # Пока нам не нужно
    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    print(f'task number is {i}')
    return 'Whatever you return gets printed in the logs'





with DAG(
    'hw_3_de-jakovlev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{i}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
        """
    )
    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_{i}',
            bash_command=templated_command,
        )
    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_the_data_{i}',
            python_callable=print_context,
            op_kwargs={'i': i},
        )
    
    """
    t3 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    """

    t1.doc_md = dedent (
        """
        Example
        """
    )
    t1 >> t2








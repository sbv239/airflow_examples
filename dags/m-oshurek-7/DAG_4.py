from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent



with DAG(
    # имя дага, которое отразиться на сервере airflow
    'DAG_4_oshurek',
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
    tags=['example_4_oshurek'],
) as dag:
    dag.doc_md = '''
    ### ignatev_dag_02 documentation

    Это есть документация для `дага` *DAG_3*, *lesson 13* in **StartML** course'''

    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )
    
    t1 = BashOperator(
        task_id = 'print_id',
        
        bash_command=templated_command,
    )
                
    t1
    

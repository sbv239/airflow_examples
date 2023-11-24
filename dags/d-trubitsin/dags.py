from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

with DAG(
    'hw_d-trubitsin_2',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 23),
    catchup=False,
    tags=['d-trubitsin_2'],
) as dag:
    # вот так можно попросить Airflow подставить логическую дату
    # в формате YYYY-MM-DD
    date = "{{ ds }}"
    t1 = BashOperator(
        task_id="task_1",
        bash_command="pwd ",  # обратите внимание на пробел в конце!
        # пробел в конце нужен в случае BashOperator из-за проблем с шаблонизацией
        # вики на проблему https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=62694614
        # и обсуждение https://github.com/apache/airflow/issues/1017
        dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
        env={"DATA_INTERVAL_START": date},  # задает переменные окружения
    )

    def print_context(ds, **kwargs):
        """Пример PythonOperator"""
        # Через синтаксис **kwargs можно получить словарь
        # с настройками Airflow. Значения оттуда могут пригодиться.
        # Пока нам не нужно
        print(kwargs)
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'Test print for "kwargs" and "ds"'

    t2 = PythonOperator(
        task_id='task_2',  # нужен task_id, как и всем операторам
        # свойственен только для PythonOperator - передаем саму функцию
        python_callable=print_context,
    )

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from airflow import DAG

with DAG(
    'first_task',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5),
    },
    description='my_first_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 20),
    catchup=False,
    tags=['example'],
) as dag:

    date = "{{ ds }}"
    tB = BashOperator(
        task_id="test_env",
        bash_command="pwd ",  # обратите внимание на пробел в конце!
        # пробел в конце нужен в случае BashOperator из-за проблем с шаблонизацией
        # вики на проблему https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=62694614
        # и обсуждение https://github.com/apache/airflow/issues/1017
        dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
        env={"DATA_INTERVAL_START": date},  # задает переменные окружения
    )


    # PythonOperator
    def print_date(ds):
        """Пример PythonOperator, вывод даты"""
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'some date'

    tP = PythonOperator(
        task_id='print_the_date',  # нужен task_id, как и всем операторам
        python_callable=print_date,  # свойственен только для PythonOperator - передаем саму функцию
    )

    tB >> tP
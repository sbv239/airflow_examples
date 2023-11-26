from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
with DAG(
    'hw_d-sysuev-38_1',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    # Описание DAG (не тасок, а самого DAG)
    description='hw_2',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 11, 25),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['hw_2'],
) as dag:

    date = "{{ ds }}"
    t1 = BashOperator(
        task_id="hw_2_t1_bush",
        bash_command="pwd ",  # обратите внимание на пробел в конце!
        # пробел в конце нужен в случае BashOperator из-за проблем с шаблонизацией
        # вики на проблему https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=62694614
        # и обсуждение https://github.com/apache/airflow/issues/1017
        dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
        env={"DATA_INTERVAL_START": date},  # задает переменные окружения
    )


    def print_context(ds):
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'Whatever you return gets printed in the logs'


    t2 = PythonOperator(
        task_id='hw_2_t2_python',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    # передаём последовательность задач
    t1 >> t2
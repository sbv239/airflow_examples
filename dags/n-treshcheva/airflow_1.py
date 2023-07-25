"""
Напишите DAG, который будет содержать BashOperator и PythonOperator. В функции PythonOperator примите
 аргумент ds и распечатайте его. Можете распечатать дополнительно любое другое сообщение.

В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow.
Результат может оказаться неожиданным, не пугайтесь - Airflow может запускать ваши задачи на разных
машинах или контейнерах с разными настройками и путями по умолчанию.

Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator.

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
with DAG(
    '11-1-treshcheva',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:

    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='hw_n-treshcheva_1',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )



    def print_context(ds, **kwargs):
    """Пример PythonOperator"""
    # Через синтаксис **kwargs можно получить словарь
    # с настройками Airflow. Значения оттуда могут пригодиться.
    # Пока нам не нужно
        print(kwargs)
    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'Whatever you return gets printed in the logs'


    run_this = PythonOperator(
        task_id='hw_n-treshcheva_2',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

t1>>t2


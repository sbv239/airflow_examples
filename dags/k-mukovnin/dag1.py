

"""
Test of documetation. Hello Yopta
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
with DAG(
    '2_mukovnin',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2023, 1, 29),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:

    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='2_bash_mukovnin',  # id, будет отображаться в интерфейсе
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


    t2 = PythonOperator(
        task_id='2_python_',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t2.doc_md = dedent(
        """\
    #### docs for 2 operator
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )  # dedent - это особенность Airflow, в него нужно оборачивать всю доку

    dag.doc_md = __doc__  # Можно забрать докстрингу из начала файла вот так
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # а можно явно написать
    # формат ds: 2021-12-25

    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
    # будет выглядеть вот так
    #      -> t2
    #  t1 |
    #      -> t3
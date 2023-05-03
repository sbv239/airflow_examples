from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG # Для объявления DAG нужно импортировать класс из airflow

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'IM_DAG_3',
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
        description='My (IM) first DAG',
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
        tags=['IM_DAG_3'],
) as dag:
    # # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    # t1 = BashOperator(
    #     task_id='print_date',  # id, будет отображаться в интерфейсе
    #     bash_command='date',  # какую bash команду выполнить в этом таске
    # )
    #
    # t2 = BashOperator(
    #     task_id='sleep',
    #     depends_on_past=False,  # переопределили настройку из DAG
    #     bash_command='sleep 5',
    #     retries=3,  # тоже переопределили retries (было 1)
    # )
    # t1.doc_md = dedent(
    #     """\
    # #### Task Documentation
    # You can document your task using the attributes `doc_md` (markdown),
    # `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    # ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    #
    # """
    # ) # dedent - это особенность Airflow, в него нужно оборачивать всю доку
    #
    # dag.doc_md = __doc__  # Можно забрать докстрингу из начала файла вот так
    # dag.doc_md = """
    # This is a documentation placed anywhere
    # """  # а можно явно написать
    # # формат ds: 2021-12-25
    # templated_command = dedent(
    #     """
    # {% for i in range(5) %}
    #     echo "{{ ds }}"
    #     echo "{{ macros.ds_add(ds, 7)}}"
    # {% endfor %}
    # """
    # )  # поддерживается шаблонизация через Jinja
    # # https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#concepts-jinja-templating
    #
    # t3 = BashOperator(
    #     task_id='templated',
    #     depends_on_past=False,
    #     bash_command=templated_command,
    # )
    #
    # def print_context(ds, **kwargs):
    #     """Пример PythonOperator"""
    #     # Через синтаксис **kwargs можно получить словарь
    #     # с настройками Airflow. Значения оттуда могут пригодиться.
    #     # Пока нам не нужно
    #     print(kwargs)
    #     # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    #     print(ds)
    #     return 'Whatever you return gets printed in the logs'
    #
    # t4 = PythonOperator(
    #     task_id='print_the_context',  # нужен task_id, как и всем операторам
    #     python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    # )
    #
    # # А вот так в Airflow указывается последовательность задач
    # t1 >> [t2, t3] >> t4
    # # будет выглядеть вот так
    # #      -> t2
    # #  t1 |
    # #      -> t3

    # def print_date(ds, **kwargs):
    #     # Через синтаксис **kwargs можно получить словарь
    #     # с настройками Airflow. Значения оттуда могут пригодиться.
    #     # Пока нам не нужно
    #     # print(kwargs)
    #     # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    #     print(ds)
    #     return ds
    #
    # t1 = PythonOperator(
    #     task_id = 't1_id_1',
    #     python_callable = print_date,
    # )
    #
    # t2 = BashOperator(
    #     task_id='t2_id_2',  # id, будет отображаться в интерфейсе
    #     bash_command='pwd',
    # )
    #

    for i in range(10):
        t1 = BashOperator(
            task_id='IM_t1_id3'+str(i),
            bash_command=f"echo {i}",
        )

    def print_number(op_kwargs):
        # print(kwargs)
        print(f"task number is: {op_kwargs(i)}")

    for i in range(20):
        t2 = PythonOperator(
            task_id='IM_t2_id4'+str(i),
            python_callable=print_number,
            op_kwargs={'i': i}
        )

    t1 >> t2
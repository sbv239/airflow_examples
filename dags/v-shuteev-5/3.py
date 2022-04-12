from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'shuteev_my_dag_3',
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
    description='My Hyper DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['shuteev'],
) as dag:
    dag.doc_md = __doc__  # Можно забрать докстрингу из начала файла вот так
    dag.doc_md = """
    This is a documentation placed anywhere
    Check this out!
    """
    def my_print_function(ds):
        print(f'task number is: {ds}')
        return 0

    for i in range(0,10):
        taskx = BashOperator(
            task_id='loop_bash_' + str(i),
            depends_on_past=False,
            bash_command=f"echo {i}",
        )
        # tloop0 >> taskx

        taskx.doc_md = dedent(
            """\
        # MyTest kek
        *Some italic*
        **Some bold**
        Something Else
        `Some code: for i in range(5): print(i)`
        # New paragraph
        Again text

        #### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc2` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

        """
        )
    for i in range(10,30):
        taskp = PythonOperator(
            task_id='my_loop_py_'+ str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=my_print_function,
            op_kwargs={'ds': str(i)},
        )
        taskp.doc_md = dedent(
            """\
        # MyTest
        *Some italic*
        **Some bold**
        Something Else
        `Some code: for i in range(5): print(i)`
        # New paragraph
        Again text
        
        #### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc2` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

        """
        )  # dedent - это особенность Airflow, в него нужно оборачивать всю доку
    # А вот так в Airflow указывается последовательность задач
    # t2 >> taskp
    # t1 >> [t2, t3] >> taskp
    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3
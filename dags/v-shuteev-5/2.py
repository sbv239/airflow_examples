from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'shuteev_my_dag_2',
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
    def my_print_function(ds):
        print(ds)
        return 0

    taskp = PythonOperator(
        task_id='my_py_print',  # в id можно делать все, что разрешают строки в python
        python_callable=my_print_function,
        op_kwargs={'ds': 'hi'},
    )


    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='python_print_date',  # id, будет отображаться в интерфейсе
        bash_command='date',  # какую bash команду выполнить в этом таске
    )

    t2 = BashOperator(
        task_id='print_pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    tloop0 = BashOperator(
        task_id='loop_bash_0',
        depends_on_past=False,
        bash_command=f"echo 0",
    )
    for i in range(1,10):
        taskx = BashOperator(
            task_id='loop_bash_' + str(i),
            depends_on_past=False,
            bash_command=f"echo {i}",
        )
        # tloop0 >> taskx

    for i in range(10,30):
        taskp = PythonOperator(
            task_id='my_loop_py_'+ str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=my_print_function,
            op_kwargs={'ds': str(i)},
        )
    # А вот так в Airflow указывается последовательность задач
    # t2 >> taskp
    # t1 >> [t2, t3] >> taskp
    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3
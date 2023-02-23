"""
#### Task Documentation

"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'a-azarova-17_hw7',
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
    description="""Создайте новый DAG и объявите в нем 30 задач. 
    Первые 10 задач сделайте типа BashOperator и выполните в них произвольную команду, 
    так или иначе использующую переменную цикла (например, можете указать f"echo {i}").
    Оставшиеся 20 задач должны быть PythonOperator, 
    при этом функция должна задействовать переменную из цикла. 
    Вы можете добиться этого, если передадите переменную через op_kwargs и примете ее на стороне функции. 
    Функция должна печатать "task number is: {task_number}", где task_number - номер задания из цикла. 
    
    Добавьте в PythonOperator из второго задания (где создавали 30 операторов в цикле) kwargs 
    и передайте в этот kwargs task_number со значением переменной цикла. 
    Также добавьте прием аргумента ts и run_id в функции, указанной в PythonOperator, и распечатайте эти значения.
    """,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['azarova'],
) as dag:

    def print_task_number(task_number, ts, run_id, **kwards):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)


    # for i in range(10):
    #     task_b = BashOperator(
    #         task_id=f't_{i}',
    #         depends_on_past=False,
    #         # bash_command=f'echo {i}',
    #         bash_command="echo $NUMBER",
    #         env={"NUMBER": i},  # задает переменные окружения
    #     )

    def get_conn(ts, run_id):
        from airflow.providers.postgres.operators.postgres import PostgresHook
        from psycopg2.extras import RealDictCursor
        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """select user_id, count(1)
                        from feed_action
                        where action='like' 
                        group by 1
                        order by 2 desc
                        limit 1
                    """
                )
                return cursor.fetchone()
        print(ts)
        print(run_id)



    t1 = PythonOperator(
        task_id='connect',
        python_callable=get_conn,
    )


    # последовательность задач
    t1
    # >> task_p

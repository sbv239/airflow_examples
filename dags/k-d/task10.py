"""
В лекции говорилось, что любой вывод return уходит неявно в XCom. Давайте это проверим.

Создайте новый DAG, содержащий два PythonOperator. 
Первый оператор должен вызвать функцию, возвращающую строку "Airflow tracks everything".

Второй оператор должен получить эту строку через XCom. Вспомните по лекции, какой должен быть ключ.
 Настройте правильно последовательность операторов.
"""


from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import timedelta, datetime

    


def push_foo():
    return "Airflow tracks everything"

def get_foo(ti):
    result = ti.xcom_pull(
        key='return_value',
        task_ids='get_task'
    )
    print(result)




with DAG (
    'k-d-t9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'description text',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023,1,1),
    catchup=False
) as dag:
    

    get_task = PythonOperator(
        task_id = 'get_task',
        python_callable=push_foo,
    )
    return_task = PythonOperator(
        task_id = 'return_task',
        python_callable=get_foo,
    )

    get_task >> return_task

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def push_data():
    return 'Airflow tracks everything'


'''
В функции, которая забирает из XCom нужно указать task_ids той таски, которая положила эти значения туда.
Передача task_ids может показаться странной, ведь мы и так уже передаем якобы уникальный ключ,
но это особенность XCom - предполагается, что один и тот же ключ может повторяться в разных Operator
'''


def pull_data(ti):
    output = ti.xcom_pull(
        key='return_value',
        task_ids='push'
    )
    print(output)


with DAG(
        'hw_10_s-plaksin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Implicit Xcom pull and push',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 22),
        catchup=False,
        tags=['hw_10'],
) as dag:
    push_task = PythonOperator(
        task_id='push',
        python_callable=push_data
    )
    pull_task = PythonOperator(
        task_id='pull',
        python_callable=pull_data
    )
    push_task >> pull_task

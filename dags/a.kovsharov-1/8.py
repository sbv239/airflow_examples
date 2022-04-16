from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        }
data = {"sample_xcom_key": "xcom test"}

with DAG(
        'hw_8_a.kosharov_XCom_DAG',
        # Параметры по умолчанию для тасок
        default_args=default_args,
    # Описание DAG (не тасок, а самого DAG)
    description='A cycle generated DAG of homework of Lesson 11',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['HW_8', 'a.kovsharov']
) as dag:

    def set_xcom_val(ti, **kwargs):
        for key, value in kwargs:
            ti.xcom_push(
                key = key,
                value = value
            )
            
    def get_xcom_val(ti, *keys):
            res = ti.xcom_pull(ti.xcom_pull(key=keys, task_ids='set_xcom_val'))
            print(res)
            
            
    t1 = PythonOperator(
            task_id = 'set_xcom_val',
            python_callable = set_xcom_val,
            op_kwargs = {'kwargs': data}
    )
        
    t2 = PythonOperator(
            task_id = 'get_xcom_val',
            python_callable = get_xcom_val,
            op_kwargs = {'keys': data.keys()}
    )
            
    t1 >> t2
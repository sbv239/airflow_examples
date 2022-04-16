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

    def save_xcom_text(text):
        return text
            
    def get_xcom_val(ti):
            res = ti.xcom_pull(key='return_value', task_ids='save_text_2_xcom')
            print(res)
            
            
    t1 = PythonOperator(
            task_id = 'save_text_2_xcom',
            python_callable = save_xcom_text,
            op_kwargs = {'text': 'Airflow tracks everything'}
    )
        
    t2 = PythonOperator(
            task_id = 'get_xcom_val',
            python_callable = get_xcom_val
    )
            
    t1 >> t2
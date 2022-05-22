from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
'''Сделайте новый DAG, содержащий два Python оператора. Первый PythonOperator должен класть в XCom значение "xcom test" по ключу "sample_xcom_key".

Второй PythonOperator должен доставать это значение и печатать его. Настройте правильно последовательность операторов.

Посмотрите внимательно, какие аргументы мы принимали в функции, когда работали с XCom.'''

def xcom_tracks_everything():
        return "Airflow tracks everything"

def get_this_everything_from_xcom(ti):
    value_xcom = ti.xcom_pull(
       key = 'return_value',
       task_ids='get_this_everything_from_xcom_pull' 
    )
    print(value_xcom)

with DAG(
    'hw_8_n-dmitrieva_xcom_dag',
    default_args = { # Default settings applied to all tasks
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    catchup=False
) as dag:

    task1_push = PythonOperator(
        task_id = 'get_this_everything_from_xcom_pull', #task ID
        python_callable = xcom_tracks_everything,
        )
    
    task2_pull = PythonOperator(
        task_id = "get_this_everything_from_xcom", #task ID
        python_callable = get_this_everything_from_xcom,
        )

task1_push >> task2_pull
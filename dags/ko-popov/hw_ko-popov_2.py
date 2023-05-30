from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_ko-popov_2',
    default_args={
        #Ждать ли  успеха, если запуск упал
        'depends_on_past': False,
        #кому писать
        'email': {'mdkonstantinp@gmail.com'},
        #писать ли вообще
        'email_on_failure': False,
        #Писать ли при автоматическом перезапуске
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hw_2_ko-popov_first dag',
    #с какой периодичностью запускать
    schedule_interval = timedelta(days=1), #каждый день
    #когда запустить
    #start_date=datetime(YYYY, M, D) - логическая дата(за какие даты должны были запроцесситься данные
    start_date=datetime(2023, 5, 29),
    catchup=False, #Наверстать опущенное?
    tags = ['hw_ko-popov_2'],
) as dag:

    t1 = BashOperator(
        task_id='call_pwd',
        bash_command='pwd', #какую bash команду выполнить в этом таске
    )

    def call_pwd(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever'

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable=call_pwd,
    )
    t1 >> t2

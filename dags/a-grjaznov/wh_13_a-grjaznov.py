from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
        # 'tutorial_gryaznov'
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)}
        , description='test Gryaznov'
        , schedule_interval=timedelta(days=1)
        , start_date=datetime(2023, 6, 20)
        , catchup=False
        , tags=['example']
        , dag_id='dag_12_gryaznov')as dag:
    def _choose_best_model(ti):
        from airflow.models import Variable
        result = Variable.get("is_startml")
        if result == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'


    def get_true():
        print('StartML is a starter course for ambitious people')


    def get_false():
        print('Not a startML course, sorry')


    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model)

    py_gryaz1 = PythonOperator(task_id='startml_desc'
                               , python_callable=get_true
                               )

    py_gryaz2 = PythonOperator(task_id='not_startml_desc'
                               , python_callable=get_false
                               )

    choose_best_model >> [py_gryaz1, py_gryaz2]

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

with DAG(
        'task_12_1_andreeva',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A DAG for task 12_1',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 30),
        catchup=False,
        tags=['task12_1', 'task_12_1', 'andreeva'],
) as dag:
    def choose_course():
        if Variable.get("is_startml") == "True":
            res = "startml_desc"
        else:
            res = "not_startml_desc"
        return res

        # Краткая версия
        # return "startml_desc" if Variable.get("is_startml") == "True" else "not_startml_desc"


    t1 = DummyOperator(task_id="before_branching")

    print_var = PythonOperator(
        task_id="print_var",
        python_callable=lambda: print(Variable.get("is_startml"))
        # Таска просто делает вывод строки, для этого тупо писать отдельную ф-цию, для краткости исп-ем лямбда ф-ции
    )

    t2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=choose_course
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=lambda: "StartML is a starter course for ambitious people"
        # Таска просто делает вывод строки, для этого тупо писать отдельную ф-цию, для краткости исп-ем лямбда ф-ции
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=lambda: "Not a startML course, sorry"
        # Таска просто делает вывод строки, для этого тупо писать отдельную ф-цию, для краткости исп-ем лямбда ф-ции
    )

    t5 = PythonOperator(
        task_id="after_branching",
        python_callable = lambda: "Finish!",
        trigger_rule="one_success"
    )

    t1 >> print_var >> t2 >> [t3, t4] >> t5

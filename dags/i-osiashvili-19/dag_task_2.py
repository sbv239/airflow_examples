# Напишите DAG, который будет содержать BashOperator и PythonOperator.
# В функции PythonOperator примите аргумент ds и распечатайте его.
# Можете распечатать дополнительно любое другое сообщение.
#
# В BashOperator выполните команду pwd, которая выведет директорию,
# где выполняется ваш код Airflow. Результат может оказаться неожиданным,
# не пугайтесь - Airflow может запускать ваши задачи на разных машинах или контейнерах
# с разными настройками и путями по умолчанию.
#
# Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator.
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'dag_task_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
) as dag:
    task_1 = BashOperator(
        task_id="show_directory",
        bash_command='pwd'
    )


    def return_date(ds):
        print("One strange thing, when I type DAG, I'm always typing GAD )))")
        return ds


    task_2 = PythonOperator(
        task_id='date_and_text',
        python_callable=return_date,
    )

    task_1 >> task_2

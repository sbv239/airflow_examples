# Напишите DAG, который будет содержать BashOperator и PythonOperator
# В функции PythonOperator примите аргумент ds и распечатайте его
# Можете распечатать дополнительно любое другое сообщение
# В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow
# Результат может оказаться неожиданным, не пугайтесь - Airflow может запускать
# ваши задачи на разных машинах или контейнерах с разными настройками и путями по умолчанию
# Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator
# NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_2_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = "Making DAG for 2nd task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 24),
    catchup = False,
    tags = ["2nd task"],
) as dag:

    t1 = BashOperator(
        task_id = "print_PWD",
        bash_command = "pwd"
    )

    def print_DS(ds, **kwargs):
        print(kwargs)
        print(ds)
        return "Hello"

    t2 = PythonOperator(
        task_id = "print_DS",
        python_callable = print_DS,
    )

    t1 >> t2
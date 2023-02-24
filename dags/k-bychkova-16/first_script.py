import airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

"""
Напишите DAG, который будет содержать BashOperator и PythonOperator.
В функции PythonOperator примите аргумент ds и распечатайте его. Можете распечатать дополнительно любое другое сообщение.
В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow. Результат
может оказаться неожиданным, не пугайтесь - Airflow может запускать ваши задачи на разных машинах
 или контейнерах с разными настройками и путями по умолчанию.
Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator.
"""
# вот так можно попросить Airflow подставить логическую дату
# в формате YYYY-MM-DD
#date = "{{ ds }}"
t = BashOperator(
    task_id="pwd_env",
    bash_command="/pwd "
)

def print_context(ds):
    print(ds)
    return 'Hello'

run_this = PythonOperator(
    task_id='print_the_context',  # нужен task_id, как и всем операторам
    python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
)
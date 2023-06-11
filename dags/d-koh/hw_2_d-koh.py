from airflow.operators.bash import BashOperator

date = "{{ ds }}"
t = BashOperator(
    task_id="test_pwd",
    bash_command="pwd ",  # обратите внимание на пробел в конце!
    # пробел в конце нужен в случае BashOperator из-за проблем с шаблонизацией
    # вики на проблему https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=62694614
    # и обсуждение https://github.com/apache/airflow/issues/1017
)


def print_context(ds, **kwargs):
    """Пример PythonOperator"""
    # Через синтаксис **kwargs можно получить словарь
    # с настройками Airflow. Значения оттуда могут пригодиться.
    # Пока нам не нужно
    print(kwargs)
    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    print(ds)
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='print_the_context',  # нужен task_id, как и всем операторам
    python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
)

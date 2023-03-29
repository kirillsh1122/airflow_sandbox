from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,    
    'start_date': datetime(2023, 03, 29),
    'end_date': datetime(2023, 03, 31),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def g_morning():
    print("Good Morning")

def g_day():
    print("Good day")

def g_evening():
    print("Good evening")


with DAG(
    dag_id="TASK 1",
    default_args=default_args,
    schedule_interval=timedelta(minutes=3)) as dag:
    
    task_1 = PythonOperator(
                            task_id="Good Morning",
                            python_callable=g_morning)
    
    task_2 = PythonOperator(
                            task_id="Good day",
                            python_callable=g_day)
    
    task_3 = PythonOperator(
                            task_id="Good evening",
                            python_callable=g_evening)
    
    task_1 >> task_2 >> task_3
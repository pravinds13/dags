from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import numpy as np

def fibonacci(n):
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)

def calculate_fibonacci():
    # Calculate Fibonacci sequence for a large number
    result = fibonacci(350)
    print(f"Fibonacci result: {result}")

def matrix_multiplication():
    # Perform large matrix multiplication
    size = 1000000000
    matrix_a = np.random.rand(size, size)
    matrix_b = np.random.rand(size, size)
    result = np.dot(matrix_a, matrix_b)
    print("Matrix multiplication result calculated: {result}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'high_cpu_utilization_dag',
    default_args=default_args,
    description='A DAG to cause high CPU utilization',
    schedule_interval=timedelta(days=1),
)

task1 = PythonOperator(
    task_id='calculate_fibonacci',
    python_callable=calculate_fibonacci,
    dag=dag,
)

task2 = PythonOperator(
    task_id='matrix_multiplication',
    python_callable=matrix_multiplication,
    dag=dag,
)

task1 >> task2

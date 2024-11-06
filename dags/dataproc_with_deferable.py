from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator
)
from datetime import datetime
from google.api_core.retry import Retry

default_args = {
    'start_date': datetime(2024, 11, 6),
}

with DAG('dataproc_deferrable_example',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    create_batch = DataprocCreateBatchOperator(
        task_id='create_batch',
        project_id='your-project-id',
        region='us-central1',
        batch={
            'pyspark_batch': {
                'main_python_file_uri': 'gs://your-bucket/your-script.py'
            }
        },
        batch_id='example-batch',
        deferrable=True,
        result_retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
        num_retries_if_resource_is_not_ready=3,
    )

    get_batch = DataprocGetBatchOperator(
        task_id='get_batch',
        project_id='your-project-id',
        region='us-central1',
        batch_id='example-batch'
    )

    delete_batch = DataprocDeleteBatchOperator(
        task_id='delete_batch',
        project_id='your-project-id',
        region='us-central1',
        batch_id='example-batch',
        trigger_rule='all_done'
    )

    create_batch >> get_batch >> delete_batch

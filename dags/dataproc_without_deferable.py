from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 11, 6),
}

with DAG('dataproc_example',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id='ford-743794c14d5ab9bafaac1a08',
        cluster_name='example-cluster',
        region='us-central1',
        cluster_config={
            'gce_cluster_config': {
                'subnetwork_uri': 'projects/ford-743794c14d5ab9bafaac1a08/regions/us-central1/subnetworks/sandbox-us-central1-subnet'
            },
            'master_config': {
                'num_instances': 1,
                'machine_type_uri': 'n1-standard-4',
            },
            'worker_config': {
                'num_instances': 2,
                'machine_type_uri': 'n1-standard-4',
            }
        }
    )

    submit_job = DataprocSubmitJobOperator(
        task_id='submit_job',
        project_id='ford-743794c14d5ab9bafaac1a08',
        region='us-central1',
        job={
            'placement': {
                'cluster_name': 'example-cluster'
            },
            'pyspark_job': {
                'main_python_file_uri': 'gs://your-bucket/your-script.py'
            }
        }
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id='ford-743794c14d5ab9bafaac1a08',
        cluster_name='example-cluster',
        region='us-central1',
        trigger_rule='all_done'
    )

    create_cluster >> submit_job >> delete_cluster

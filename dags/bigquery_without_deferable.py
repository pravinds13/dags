from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 11, 7),
}

with DAG('bigquery_example',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    create_table = BigQueryInsertJobOperator(
        task_id='create_table',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `ford-743794c14d5ab9bafaac1a08.dag_test_dataset.contents` AS
                    SELECT * FROM `bigquery-public-data.github_repos.contents`
                """,
                "useLegacySql": False,
            }
        },
        location='US'
    )

    for i in range(10):
        task = BigQueryInsertJobOperator(
              task_id=f'update_table_{i}',
              configuration={
                  "query": {
                      "query": """
                          INSERT INTO `ford-743794c14d5ab9bafaac1a08.dag_test_dataset.contents`
                          SELECT * FROM `bigquery-public-data.github_repos.contents`
                      """,
                      "useLegacySql": False,
                  }
              },
              location='US'
          )
        
        # Set the starting task as the upstream dependency for the first task
        if i == 0:
            create_table >> task
        #else:
            # Set the previous task as the upstream dependency for the current task
        #    previous_update_task = dag.get_task(f'update_table_{i-1}')
        #    previous_update_task >> task

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_marts_update',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define mounts using Mount objects
    mounts = [
        Mount(
            source='C:/Users/abdom/OneDrive/Desktop/Hiring/Data Engineer Orange/interview_env_shared/dbt_project',
            target='/app',
            type='bind'
        ),
        Mount(
            source='C:/Users/abdom/OneDrive/Desktop/Hiring/Data Engineer Orange/interview_env_shared/dbt_profiles',
            target='/root/.dbt',
            type='bind'
        )
    ]

    # Task to run dbt models with unique container names
    dbt_run = DockerOperator(
        task_id='dbt_run',
        container_name='dbt_{{ ts_nodash }}',  # Generate unique container name based on timestamp
        image='dbt_task_image',
        command='dbt run --profiles-dir /root/.dbt --project-dir /app',
        mounts=mounts,
        mount_tmp_dir=False,
        dag=dag,
    )

    # Task to run dbt tests to validate the marts
    dbt_test = DockerOperator(
        task_id='dbt_test',
        container_name='dbt_test_{{ ts_nodash }}',  # Generate unique container name for testing
        image='dbt_task_image',
        command='dbt test --profiles-dir /root/.dbt --project-dir /app',
        mounts=mounts,
        mount_tmp_dir=False,
        dag=dag,
    )

    # Define the task order
    dbt_run >> dbt_test

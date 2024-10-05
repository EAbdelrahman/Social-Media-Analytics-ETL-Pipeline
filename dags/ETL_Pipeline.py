from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='etl_pipeline_docker',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:


    # Task 1: Extract Data
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command='docker exec pyspark spark-submit /home/jovyan/work/Write_CSV.py',
    )

    # Task 2: Clean Data
    clean_data = BashOperator(
        task_id='clean_data',
        bash_command='docker exec pyspark spark-submit /home/jovyan/work/Cleaning.py',
    )

    # Task 3: Logging
    logging_task = BashOperator(
        task_id='logging_task',
        bash_command='docker exec pyspark spark-submit /home/jovyan/work/Logging.py',
    )

    # Task 4: Connect and Write to DWH
    connect_write_dwh = BashOperator(
        task_id='connect_write_dwh',
        bash_command='docker exec pyspark spark-submit \
        --master local[*] \
        --name "Load Data to PostgreSQL" \
        --driver-class-path /home/jovyan/drivers/postgresql-42.7.3.jar \
        --jars /home/jovyan/drivers/postgresql-42.7.3.jar \
        --driver-memory 8g \
        --executor-memory 8g \
        --conf "spark.sql.shuffle.partitions=100" \
        /home/jovyan/work/Connect_Dwh.py ' ) 

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='docker exec dbt dbt run --profiles-dir /root/.dbt --project-dir /app',
        dag=dag,
    )

    # Task to run dbt tests inside the existing dbt container
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='docker exec dbt dbt test --profiles-dir /root/.dbt --project-dir /app',
        dag=dag,
    )

    # Task Dependencies
    extract_data >> clean_data >> logging_task >> connect_write_dwh >> dbt_run >> dbt_test


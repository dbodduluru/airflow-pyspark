import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta

# Default arguments
default_args = {
    "owner": "Haris Syd",
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="pyspark_job_dag",
    default_args=default_args,
    description="Run a PySpark job using Airflow and Spark cluster",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task to log the start of the job
    start_task = PythonOperator(
        task_id="log_start",
        python_callable=lambda: print("Starting the PySpark job..."),
    )

    # SparkSubmitOperator to run the PySpark job
    spark_submit_task = SparkSubmitOperator(
        task_id="spark_submit_task",
        application="/opt/airflow/jobs/python/wordcountjob.py",  # Correct path for Spark container
        conn_id="spark_conn",
        verbose=True,
    )

    # Task to log the completion of the job
    end_task = PythonOperator(
        task_id="log_end",
        python_callable=lambda: print("PySpark job completed successfully."),
    )

    # Set task dependencies
    start_task >> spark_submit_task >> end_task

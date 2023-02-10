import datetime as dt

from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchCreateComputeEnvironmentOperator, BatchOperator

dag = DAG(
    dag_id="dagsven",
    default_args={"owner": "Airflow"},
    schedule_interval="@hourly",
    start_date=dt.datetime(2023, 2, 8),
)

with dag:
  submit_batch_job = BatchOperator(
      task_id="submit_batch_job",
      job_name="Sven-test-job",
      job_queue="arn:aws:batch:eu-west-1:338791806049:job-queue/academy-capstone-winter-2023-job-queue",
      job_definition="arn:aws:batch:eu-west-1:338791806049:job-definition/Sven-capstone-job:2",
      overrides="",
    )
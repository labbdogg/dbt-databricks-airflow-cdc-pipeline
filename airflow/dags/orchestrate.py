from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import os
import time


@dag
def orchestrate():

    @task
    def ingest_cdc():

        # Credentials come from the environment (airflow/.env, loaded by docker-compose).
        ws = WorkspaceClient(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_TOKEN"],
        )

        job_trigger = ws.jobs.run_now(job_id=int(os.environ["DATABRICKS_JOB_ID"]))

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break 
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5)  # Wait for 5 seconds before checking the job status again
        
        return "CDC Ingestion Completed"

    @task
    def source_freshness():
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id='silver_technical_tests',
        bash_command='cd /opt/airflow/walmart_project && dbt run --select silver_technical'
    )

    silver_technical_tests = BashOperator(
        task_id='silver_technical',
        bash_command='cd /opt/airflow/walmart_project && dbt test --select test_obt'
    )

    silver_business = BashOperator(
        task_id='silver_business',
        bash_command='cd /opt/airflow/walmart_project && dbt run --select obt'
    )

    gold_ephermeral = BashOperator(
        task_id='gold_ephermeral',
        bash_command='cd /opt/airflow/walmart_project && dbt run --select gold/ephermeral'
    )

    gold_dimensions = BashOperator(
        task_id='gold_dimensions',
        bash_command='cd /opt/airflow/walmart_project && dbt snapshot'
    )

    gold_facts = BashOperator(
        task_id='gold_facts',
        bash_command='cd /opt/airflow/walmart_project && --select gold/fact'
    )

    ingest_cdc() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> gold_ephermeral >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()
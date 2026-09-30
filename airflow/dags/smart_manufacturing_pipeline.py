from datetime import datetime
import pendulum
from airflow import DAG
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="smart_manufacturing_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Casablanca"),
    schedule="0 2 * * *",
    catchup=False,
    tags=["smart-manufacturing"],
) as dag:

    trigger_adf = AzureDataFactoryRunPipelineOperator(
        task_id="trigger_adf_master_pipeline",
        azure_data_factory_conn_id="azure_smart_manufacturing",
        factory_name="adf-smart-manufacturing",
        resource_group_name="rg-smart-manufacturing",
        pipeline_name="pl_master_orchestrator",
        wait_for_termination=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="""
            cd /opt/airflow/dbt
            dbt build
        """,
    )

    trigger_adf >> dbt_build
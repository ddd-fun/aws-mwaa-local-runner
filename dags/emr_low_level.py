from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor


EmrCreateJobFlowOperator.ui_color = '#ffe4b2'
EmrStepSensor.ui_color = '#ffe4b2'
EmrTerminateJobFlowOperator.ui_color = '#ffe4b2'
EmrAddStepsOperator.ui_color = "#f59e9e"
EmrJobFlowSensor.ui_color = '#ffe4b2'



S3_ARTIFACT = "s3://as24data-artifacts/mesh-team/pipeline/"
S3_DATA_LOCATION = "s3://as24data-processed/mesh-team/pipeline/"

JOB_FLOW_ROLE = 'DataWarioDefaultPipelineResourceRole'
SERVICE_ROLE = 'DataWarioDefaultPipelineRole'

JOB_FLOW_OVERRIDES = {
    'Name': 'PiCalc',
    'ReleaseLabel': 'emr-5.29.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'JobFlowRole': JOB_FLOW_ROLE,
    'ServiceRole': SERVICE_ROLE,
}


MY_SPARK_STEPS = [
    {
        "Name": "process-salesforce-objects",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--class",
                "com.autoscout24.data.salesforceObjectProcessor.Main",
                "{{params.mySalesforceObjectProcessorArtifactPath}}",
                "--config-file-path",
                "/home/hadoop/artifacts/object-processor-config.yaml",
                "--range-start-date",
                "{{params.myStartDate}}",
                "--range-end-date",
                "{{params.myEndDate}}"
            ],
        },
    },
]

PUBLISH_TABLE_SPARK_UTILS = [
    {
        "Name": "process-salesforce-objects",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--class",
                "com.autoscout24.data.publishTableWrapper",
                "{{params.mySalesforceObjectProcessorArtifactPath}}",
                "--config-file-path",
                "s3://as24-data-artifacts/datawario/my-team/artifacts/publish",
                "--range-start-date",
                "{{params.myStartDate}}",
                "--range-end-date",
                "{{params.myEndDate}}"
            ],
        },
    },
]


default_args = {
    "owner": "data_mesh_team",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["data_mesh_team@aautoscout24.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG("emr_low_level_api", default_args=default_args, schedule_interval=timedelta(1)) as dag:

    job_flow_creator = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    job_sensor = EmrJobFlowSensor(
        task_id='wait_for_emr_cluster',
        job_flow_id=job_flow_creator.output
    )

    spark_app_step = EmrAddStepsOperator(
        task_id="submit_spark_app",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value')}}",
        steps=MY_SPARK_STEPS,
    )

    spark_app_checker = EmrStepSensor(
        task_id=f'wait_for_my_spark_app',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value')}}",
        step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_app', key='return_value')[0] }}",
    )

    publish_table_step = EmrAddStepsOperator(
        task_id="publish_table",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value')}}",
        steps=PUBLISH_TABLE_SPARK_UTILS,
    )

    publish_table_step_checker = EmrStepSensor(
        task_id=f'wait_for_publish_table',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value')}}",
        step_id="{{ task_instance.xcom_pull(task_ids='spark_steps', key='return_value')[0] }}",
    )
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='terminate_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='publish_table', key='return_value') }}",
    )

    op2 = BashOperator(task_id=f"quality_checks", bash_command=f"echo 'team_quality_cheks'")

job_flow_creator >> job_sensor >> spark_app_step >> spark_app_checker >> publish_table_step >> publish_table_step_checker >> cluster_remover >> op2
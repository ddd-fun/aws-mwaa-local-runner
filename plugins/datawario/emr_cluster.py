from airflow.contrib.operators.emr_create_job_flow_operator import (
    EmrCreateJobFlowOperator,
)
from airflow.operators.bash import BashOperator

from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor


JOB_FLOW_ROLE = 'DataWarioDefaultPipelineResourceRole'
SERVICE_ROLE = 'DataWarioDefaultPipelineRole'

SPARK_PI_STEPS = [
    {
        'Name': 'calculate_pi',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],

        },
    }
]

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


class EmrCluster:

    def __init__(self, dag):
       self.dag = dag
       self.job_flow_creator = None
       self.flow = None

    def __enter__(self):
        self.job_flow_creator = EmrCreateJobFlowOperator(
            task_id='create_job_flow',
            job_flow_overrides=JOB_FLOW_OVERRIDES,
            dag = self.dag
        )
        job_sensor = EmrJobFlowSensor(
            task_id='wait_for_job_flow',
            job_flow_id=self.job_flow_creator.output,
            dag = self.dag
        )
        self.flow = job_sensor
        return self

    def add_spark_steps(self, spark_steps):
        step_adder = EmrAddStepsOperator(
            task_id='spark_steps',
            job_flow_id=self.job_flow_creator.output,
            steps=spark_steps,
        )
        step_checker = EmrStepSensor(
            task_id='wait_for_spark_steps',
            job_flow_id=self.job_flow_creator.output,
            step_id="{{ task_instance.xcom_pull(task_ids='spark_steps', key='return_value')[0] }}",
        )
        self.flow = self.flow >> step_adder >> step_checker

    def get_emr_flow(self):
        return self.flow

    def __exit__(self, exc_type, exc_value, exc_traceback):
        cluster_remover = EmrTerminateJobFlowOperator(
            task_id='remove_cluster',
            job_flow_id=self.job_flow_creator.output,
        )
        self.flow = self.flow >> cluster_remover
        return False

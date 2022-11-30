from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from datawario.emr_cluster import (EmrCluster, SparkUtils)

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

S3_ARTIFACT = "s3://as24data-artifacts/mesh-team/pipeline/"
S3_DATA_LOCATION = "s3://as24data-processed/mesh-team/pipeline/"

with DAG("emr_python_abstraction", default_args=default_args, schedule_interval=timedelta(1), tags=['workshop']) as dag:
    # infrastructure workflow is behind EmrCluster abstraction!
    with EmrCluster(dag) as emr:
        # here only app flow is defined
        emr.add_spark_steps("spark_transform", SparkUtils.spark_app(S3_ARTIFACT, "com.as24.meshteam.SparkApp"))
        emr.add_spark_steps("spark_publish_result", SparkUtils.publish_hive_table(S3_DATA_LOCATION, "mesh_team", "table_1"))

    # we could extend workflow with extra steps
    op2 = BashOperator(task_id=f"quality_checks", bash_command=f"echo 'team_quality_cheks'")

    emr.get_emr_flow() >> op2

from typing import Any

import boto3

from ADF.config import ADFGlobalConfig


try:
    from mypy_boto3_s3 import Client as S3Client
    from mypy_boto3_iam import Client as IAMClient
    from mypy_boto3_rds import Client as RDSClient
    from mypy_boto3_emr import Client as EMRClient
    from mypy_boto3_emr_serverless import Client as EMRServerlessClient
    from mypy_boto3_ec2 import Client as EC2Client
    from mypy_boto3_sqs import Client as SQSClient
    from mypy_boto3_lambda import Client as LambdaClient
    from mypy_boto3_athena import Client as AthenaClient
    from mypy_boto3_redshift import Client as RedshiftClient
except:
    S3Client = Any
    IAMClient = Any
    RDSClient = Any
    EMRClient = Any
    EMRServerlessClient = Any
    EC2Client = Any
    SQSClient = Any
    LambdaClient = Any
    AthenaClient = Any
    RedshiftClient = Any

s3_resource = boto3.resource("s3", region_name=ADFGlobalConfig.AWS_REGION)
ec2_resource = boto3.resource("ec2", region_name=ADFGlobalConfig.AWS_REGION)
iam_resource = boto3.resource("iam", region_name=ADFGlobalConfig.AWS_REGION)

s3_client: S3Client = boto3.client("s3", region_name=ADFGlobalConfig.AWS_REGION)
iam_client: IAMClient = boto3.client("iam", region_name=ADFGlobalConfig.AWS_REGION)
rds_client: RDSClient = boto3.client("rds", region_name=ADFGlobalConfig.AWS_REGION)
emr_client: EMRClient = boto3.client("emr", region_name=ADFGlobalConfig.AWS_REGION)
try:
    emr_serverless_client: EMRServerlessClient = boto3.client(
        "emr-serverless", region_name=ADFGlobalConfig.AWS_REGION
    )
except:
    emr_serverless_client = None
ec2_client: EC2Client = boto3.client("ec2", region_name=ADFGlobalConfig.AWS_REGION)
sqs_client: SQSClient = boto3.client("sqs", region_name=ADFGlobalConfig.AWS_REGION)
lambda_client: LambdaClient = boto3.client(
    "lambda", region_name=ADFGlobalConfig.AWS_REGION
)
athena_client: AthenaClient = boto3.client(
    "athena", region_name=ADFGlobalConfig.AWS_REGION
)
redshift_client: RedshiftClient = boto3.client(
    "redshift", region_name=ADFGlobalConfig.AWS_REGION
)

iam_role_exists_waiter = iam_client.get_waiter("role_exists")
iam_instance_profile_exists_waiter = iam_client.get_waiter("instance_profile_exists")
vpc_available_waiter = ec2_client.get_waiter("vpc_available")
security_group_exists_waiter = ec2_client.get_waiter("security_group_exists")
subnet_available_waiter = ec2_client.get_waiter("subnet_available")
nat_gateway_available_waiter = ec2_client.get_waiter("nat_gateway_available")
rds_db_instance_available_waiter = rds_client.get_waiter("db_instance_available")
emr_cluster_running_waiter = emr_client.get_waiter("cluster_running")
redshift_available_waiter = redshift_client.get_waiter("cluster_available")

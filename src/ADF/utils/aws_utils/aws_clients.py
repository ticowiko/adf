import boto3

from ADF.config import ADFGlobalConfig


s3_resource = boto3.resource("s3", region_name=ADFGlobalConfig.AWS_REGION)
ec2_resource = boto3.resource("ec2", region_name=ADFGlobalConfig.AWS_REGION)
iam_resource = boto3.resource("iam", region_name=ADFGlobalConfig.AWS_REGION)

s3_client = boto3.client("s3", region_name=ADFGlobalConfig.AWS_REGION)
iam_client = boto3.client("iam", region_name=ADFGlobalConfig.AWS_REGION)
rds_client = boto3.client("rds", region_name=ADFGlobalConfig.AWS_REGION)
emr_client = boto3.client("emr", region_name=ADFGlobalConfig.AWS_REGION)
ec2_client = boto3.client("ec2", region_name=ADFGlobalConfig.AWS_REGION)
sqs_client = boto3.client("sqs", region_name=ADFGlobalConfig.AWS_REGION)
lambda_client = boto3.client("lambda", region_name=ADFGlobalConfig.AWS_REGION)
redshift_client = boto3.client("redshift", region_name=ADFGlobalConfig.AWS_REGION)
redshift_data_client = boto3.client(
    "redshift-data", region_name=ADFGlobalConfig.AWS_REGION
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

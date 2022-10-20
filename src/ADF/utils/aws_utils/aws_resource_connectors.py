import os
import sys
import time
import json
import logging

try:
    import mypy_boto3_ec2
except ModuleNotFoundError:
    pass

from abc import ABC, abstractmethod
from typing import Dict, Optional, List
from typing_extensions import Literal

from ADF.exceptions import AWSFailedConfigFetch
from ADF.utils.system_utils import run_command
from ADF.utils.aws_utils import (
    ec2_resource,
    iam_resource,
    iam_client,
    rds_client,
    emr_client,
    emr_serverless_client,
    ec2_client,
    sqs_client,
    lambda_client,
    redshift_client,
    iam_role_exists_waiter,
    iam_instance_profile_exists_waiter,
    vpc_available_waiter,
    nat_gateway_available_waiter,
    rds_db_instance_available_waiter,
    emr_cluster_running_waiter,
    redshift_available_waiter,
    AWSResourceConfig,
    AWSIAMRoleConfig,
    AWSInstanceProfileConfig,
    AWSVPCConfig,
    AWSInternetGatewayConfig,
    AWSNATGatewayConfig,
    AWSVPCEndpointConfig,
    AWSElasticIPConfig,
    AWSRouteTableConfig,
    AWSSecurityGroupConfig,
    AWSSubnetConfig,
    AWSSubnetGroupConfig,
    AWSClusterSubnetGroupConfig,
    AWSRDSConfig,
    AWSSQSConfig,
    AWSEventSourceMappingConfig,
    AWSLambdaConfig,
    AWSEMRConfig,
    AWSEMRServerlessConfig,
    AWSRedshiftConfig,
)


class AWSResourceConnector(ABC):
    @abstractmethod
    def requires_update(self, config: AWSResourceConfig) -> bool:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    # Leave out return type hint to allow for dynamic setting in usage
    @abstractmethod
    def fetch_config(self):
        pass

    # Leave out return type hint to allow for dynamic setting in usage
    def force_fetch_config(self):
        config = self.fetch_config()
        if config is None:
            raise AWSFailedConfigFetch(self)
        return config

    @abstractmethod
    def create(self) -> AWSResourceConfig:
        pass

    @abstractmethod
    def update(self, config: AWSResourceConfig) -> AWSResourceConfig:
        pass

    # Leave out return type hint to allow for dynamic setting in usage
    def update_or_create(self):
        config = self.fetch_config()
        if config is None:
            logging.info(f"Creating '{self}'...")
            return self.create()
        elif self.requires_update(config):
            logging.info(f"Updating '{self}'...")
            return self.update(config)
        else:
            logging.info(f"Skipping update for '{self}'...")
            return config

    def destroy_if_exists(self, must_exist=False) -> None:
        config = self.fetch_config()
        if config is not None:
            logging.info(f"Destroying '{str(self)}'...")
            self.destroy(config)
        elif must_exist:
            raise ValueError(
                f"Could not forcibly destroy {str(self)} as it does not exist."
            )
        else:
            logging.info(f"Skipping '{str(self)}' destruction as it does not exist...")

    @abstractmethod
    def destroy(self, config: AWSResourceConfig) -> None:
        pass


class AWSIAMRoleConnector(AWSResourceConnector):
    def __init__(
        self,
        role_name: str,
        assume_role_policy_document: Optional[Dict] = None,
        policy_document: Optional[Dict] = None,
    ):
        super().__init__()
        self.role_name = role_name
        self.assume_role_policy_document = assume_role_policy_document or {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": [
                            "sqs.amazonaws.com",
                            "lambda.amazonaws.com",
                            "elasticmapreduce.amazonaws.com",
                            "emr-serverless.amazonaws.com",
                            "ec2.amazonaws.com",
                            "redshift.amazonaws.com",
                            "s3.amazonaws.com",
                        ]
                    },
                    "Action": ["sts:AssumeRole"],
                },
            ],
        }
        self.policy_document = policy_document or {
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Action": "*",
                "Resource": "*",
            },
        }

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSIAMRoleConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.role_name}"

    def fetch_config(self) -> Optional[AWSIAMRoleConfig]:
        try:
            return AWSIAMRoleConfig.from_response(
                iam_client.get_role(
                    RoleName=self.role_name,
                )["Role"]
            )
        except iam_client.exceptions.NoSuchEntityException:
            return None

    # User end security configuration modifications should not be reset
    def update(self, config: AWSIAMRoleConfig) -> AWSIAMRoleConfig:
        return config

    def create(self) -> AWSIAMRoleConfig:
        AWSIAMRoleConfig.from_response(
            iam_client.create_role(
                RoleName=self.role_name,
                AssumeRolePolicyDocument=json.dumps(self.assume_role_policy_document),
            )["Role"]
        )
        iam_client.put_role_policy(
            RoleName=self.role_name,
            PolicyName=f"{self.role_name}-admin-rights",
            PolicyDocument=json.dumps(self.policy_document),
        )
        iam_role_exists_waiter.wait(RoleName=self.role_name)
        return self.force_fetch_config()

    def destroy(self, config: AWSIAMRoleConfig) -> None:
        for policy in iam_resource.Role(name=self.role_name).policies.all():
            policy.delete()
        iam_client.delete_role(RoleName=self.role_name)


class AWSVPCConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        cidr_block: str,
    ):
        super().__init__()
        self.name = name
        self.cidr_block = cidr_block

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSVPCConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSVPCConfig]:
        response = ec2_client.describe_vpcs(
            Filters=[
                {
                    "Name": "tag:Name",
                    "Values": [self.name],
                },
            ],
        )
        if len(response["Vpcs"]) == 0:
            return None
        elif len(response["Vpcs"]) == 1:
            return AWSVPCConfig.from_response(response["Vpcs"][0])
        else:
            raise ValueError(f"Found multiple VPCs with name {self.name}.")

    def create(self) -> AWSVPCConfig:
        ec2_client.create_vpc(
            CidrBlock=self.cidr_block,
            TagSpecifications=[
                {
                    "ResourceType": "vpc",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": self.name,
                        },
                    ],
                },
            ],
        )
        vpc_available_waiter.wait(
            Filters=[
                {
                    "Name": "tag:Name",
                    "Values": [self.name],
                },
            ],
        )
        config = self.force_fetch_config()
        ec2_client.modify_vpc_attribute(
            VpcId=config.vpc_id, EnableDnsSupport={"Value": True}
        )
        ec2_client.modify_vpc_attribute(
            VpcId=config.vpc_id, EnableDnsHostnames={"Value": True}
        )
        return config

    # User end security configuration modifications should not be reset
    def update(self, config: AWSVPCConfig) -> AWSVPCConfig:
        return config

    def destroy(self, config: AWSVPCConfig) -> None:
        ec2_client.delete_vpc(VpcId=config.vpc_id)


class AWSInternetGatewayConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        vpc_id: str,
    ):
        self.name = name
        self.vpc_id = vpc_id

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSInternetGatewayConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSInternetGatewayConfig]:
        response = ec2_client.describe_internet_gateways(
            Filters=[{"Name": "tag:Name", "Values": [self.name]}]
        )
        if len(response["InternetGateways"]) == 0:
            return None
        elif len(response["InternetGateways"]) == 1:
            return AWSInternetGatewayConfig.from_response(
                response["InternetGateways"][0]
            )
        else:
            raise ValueError(f"Found multiple internet gateways with name {self.name}.")

    def create(self) -> AWSInternetGatewayConfig:
        config = AWSInternetGatewayConfig.from_response(
            ec2_client.create_internet_gateway(
                TagSpecifications=[
                    {
                        "ResourceType": "internet-gateway",
                        "Tags": [{"Key": "Name", "Value": self.name}],
                    },
                ],
            )["InternetGateway"]
        )
        ec2_client.attach_internet_gateway(
            InternetGatewayId=config.internet_gateway_id,
            VpcId=self.vpc_id,
        )
        return config

    # User end security configuration modifications should not be reset
    def update(self, config: AWSInternetGatewayConfig) -> AWSInternetGatewayConfig:
        return config

    def destroy(self, config: AWSInternetGatewayConfig) -> None:
        ec2_client.delete_internet_gateway(InternetGatewayId=config.internet_gateway_id)


class AWSNATGatewayConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        subnet_id: str,
        public: bool = True,
        allocation_id: Optional[str] = None,
    ):
        self.name = name
        self.subnet_id = subnet_id
        self.allocation_id = allocation_id
        self.public = public

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSNATGatewayConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSNATGatewayConfig]:
        response = ec2_client.describe_nat_gateways(
            Filters=[{"Name": "tag:Name", "Values": [self.name]}]
        )
        if len(response["NatGateways"]) == 0:
            return None
        elif len(response["NatGateways"]) == 1:
            return AWSNATGatewayConfig.from_response(response["NatGateways"][0])
        else:
            raise ValueError(f"Found multiple nat gateways with name {self.name}.")

    def create(self) -> AWSNATGatewayConfig:
        nat_gateway_id = ec2_client.create_nat_gateway(
            SubnetId=self.subnet_id,
            ConnectivityType="public" if self.public else "private",
            TagSpecifications=[
                {
                    "ResourceType": "natgateway",
                    "Tags": [{"Key": "Name", "Value": self.name}],
                },
            ],
            **(
                {}
                if self.allocation_id is None
                else {"AllocationId": self.allocation_id}
            ),
        )["NatGateway"]["NatGatewayId"]
        nat_gateway_available_waiter.wait(NatGatewayIds=nat_gateway_id)
        return self.force_fetch_config()

    # User end security configuration modifications should not be reset
    def update(self, config: AWSNATGatewayConfig) -> AWSNATGatewayConfig:
        return config

    def destroy(self, config: AWSNATGatewayConfig) -> None:
        ec2_client.delete_nat_gateway(NatGatewayId=config.nat_gateway_id)


class AWSVPCEndpointConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        vpc_id: str,
        service_name: str,
        endpoint_type: Literal["Gateway", "GatewayLoadBalancer", "Interface"],
        route_table_ids: List[str],
        subnet_ids: List[str],
        private_dns_enabled: bool,
    ):
        self.name = name
        self.vpc_id = vpc_id
        self.service_name = service_name
        self.endpoint_type = endpoint_type
        self.route_table_ids = route_table_ids
        self.subnet_ids = subnet_ids
        self.private_dns_enabled = private_dns_enabled

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSVPCEndpointConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSVPCEndpointConfig]:
        response = ec2_client.describe_vpc_endpoints(
            Filters=[{"Name": "tag:Name", "Values": [self.name]}]
        )
        if len(response["VpcEndpoints"]) == 0:
            return None
        elif len(response["VpcEndpoints"]) == 1:
            return AWSVPCEndpointConfig.from_response(response["VpcEndpoints"][0])
        else:
            raise ValueError(f"Found multiple vpc endpoints with name {self.name}.")

    def create(self) -> AWSVPCEndpointConfig:
        ec2_client.create_vpc_endpoint(
            VpcEndpointType=self.endpoint_type,
            VpcId=self.vpc_id,
            ServiceName=self.service_name,
            RouteTableIds=self.route_table_ids,
            SubnetIds=self.subnet_ids,
            PrivateDnsEnabled=self.private_dns_enabled,
            TagSpecifications=[
                {
                    "ResourceType": "vpc-endpoint",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": self.name,
                        },
                    ],
                },
            ],
        )
        time.sleep(5)
        return self.force_fetch_config()

    # User end security configuration modifications should not be reset
    def update(self, config: AWSVPCEndpointConfig) -> AWSVPCEndpointConfig:
        return config

    def destroy(self, config: AWSVPCEndpointConfig) -> None:
        ec2_client.delete_vpc_endpoints(VpcEndpointIds=[config.vpc_endpoint_id])
        while self.fetch_config() is not None:
            time.sleep(5)


class AWSElasticIPConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
    ):
        self.name = name

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSElasticIPConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSElasticIPConfig]:
        response = ec2_client.describe_addresses(
            Filters=[{"Name": "tag:Name", "Values": [self.name]}]
        )
        if len(response["Addresses"]) == 0:
            return None
        elif len(response["Addresses"]) == 1:
            return AWSElasticIPConfig.from_response(response["Addresses"][0])
        else:
            raise ValueError(f"Found multiple nat gateways with name {self.name}.")

    def create(self) -> AWSElasticIPConfig:
        ec2_client.allocate_address(
            Domain="vpc",
            TagSpecifications=[
                {
                    "ResourceType": "elastic-ip",
                    "Tags": [{"Key": "Name", "Value": self.name}],
                },
            ],
        )
        return self.force_fetch_config()

    # User end security configuration modifications should not be reset
    def update(self, config: AWSElasticIPConfig) -> AWSElasticIPConfig:
        return config

    def destroy(self, config: AWSElasticIPConfig) -> None:
        ec2_client.release_address(AllocationId=config.allocation_id)


class AWSRouteTableConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        vpc_id: str,
    ):
        super().__init__()
        self.name = name
        self.vpc_id = vpc_id

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSRouteTableConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSRouteTableConfig]:
        response = ec2_client.describe_route_tables(
            Filters=[{"Name": "tag:Name", "Values": [self.name]}]
        )
        if len(response["RouteTables"]) == 0:
            return None
        elif len(response["RouteTables"]) == 1:
            return AWSRouteTableConfig.from_response(response["RouteTables"][0])
        else:
            raise ValueError(f"Found multiple route tables with name {self.name}.")

    def create(self) -> AWSRouteTableConfig:
        return AWSRouteTableConfig.from_response(
            ec2_client.create_route_table(
                VpcId=self.vpc_id,
                TagSpecifications=[
                    {
                        "ResourceType": "route-table",
                        "Tags": [{"Key": "Name", "Value": self.name}],
                    },
                ],
            )["RouteTable"]
        )

    # User end security configuration modifications should not be reset
    def update(self, config: AWSRouteTableConfig) -> AWSRouteTableConfig:
        return config

    def destroy(self, config: AWSRouteTableConfig) -> None:
        ec2_client.delete_route_table(RouteTableId=config.route_table_id)


class AWSSecurityGroupConnector(AWSResourceConnector):
    def __init__(
        self,
        vpc_id: str,
        group_name: str,
        description: str,
        init_ingress_rule: Optional[Literal["all_ipv4", "all_ssh"]] = "all_ipv4",
    ):
        super().__init__()
        self.vpc_id = vpc_id
        self.group_name = group_name
        self.description = description
        self.init_ingress_rule = init_ingress_rule
        if self.init_ingress_rule not in ["all_ipv4", "all_ssh", None]:
            raise ValueError(f"Unknown init ingress rule '{self.init_ingress_rule}'.")

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSSecurityGroupConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.vpc_id}:{self.group_name}"

    def fetch_resource(
        self, force=False
    ) -> Optional["mypy_boto3_ec2.service_resource.SecurityGroup"]:
        for sg in ec2_resource.Vpc(self.vpc_id).security_groups.all():
            if sg.group_name == self.group_name:
                return sg
        if force:
            raise ValueError(f"Failed to find resource for {str(self)}.")
        return None

    def fetch_config(self) -> Optional[AWSSecurityGroupConfig]:
        sg = self.fetch_resource()
        if sg is not None:
            return AWSSecurityGroupConfig.from_response(
                ec2_client.describe_security_groups(GroupIds=[sg.group_id])[
                    "SecurityGroups"
                ][0]
            )
        else:
            return None

    def create(self) -> AWSSecurityGroupConfig:
        ec2_client.create_security_group(
            Description=self.description,
            GroupName=self.group_name,
            VpcId=self.vpc_id,
        )
        if self.init_ingress_rule == "all_ipv4":
            ec2_client.authorize_security_group_ingress(
                GroupId=self.fetch_resource(force=True).group_id,
                IpPermissions=[
                    {
                        "IpProtocol": "-1",
                        "FromPort": 0,
                        "ToPort": 65535,
                        "IpRanges": [
                            {
                                "CidrIp": "0.0.0.0/0",
                                "Description": "Allow all IPv4 traffic",
                            },
                        ],
                    },
                ],
            )
        elif self.init_ingress_rule == "all_ssh":
            ec2_client.authorize_security_group_ingress(
                GroupId=self.fetch_resource(force=True).group_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 22,
                        "ToPort": 22,
                        "IpRanges": [
                            {
                                "CidrIp": "0.0.0.0/0",
                                "Description": "Allow all SSH traffic",
                            },
                        ],
                    },
                ],
            )
        elif self.init_ingress_rule is None:
            pass
        else:
            raise ValueError(f"Unknown init ingress rule '{self.init_ingress_rule}'.")
        return self.force_fetch_config()

    # User end security configuration modifications should not be reset
    def update(self, config: AWSSecurityGroupConfig) -> AWSSecurityGroupConfig:
        return config

    def reset_rules(self):
        sg = self.fetch_resource(force=True)
        if sg.ip_permissions:
            sg.revoke_ingress(IpPermissions=sg.ip_permissions)
        else:
            logging.info(
                f"Skipping rules reset for {str(self)} as they are already empty...",
            )

    def destroy(self, config: AWSSecurityGroupConfig) -> None:
        self.reset_rules()
        self.fetch_resource().delete()


class AWSSubnetConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        vpc_id: str,
        cidr_block: str,
        availability_zone: str,
        route_table_id: Optional[str] = None,
    ):
        self.name = name
        self.vpc_id = vpc_id
        self.cidr_block = cidr_block
        self.availability_zone = availability_zone
        self.route_table_id = route_table_id

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSSubnetConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.vpc_id}:{self.name}"

    def fetch_config(self) -> Optional[AWSSubnetConfig]:
        response = ec2_client.describe_subnets(
            Filters=[
                {
                    "Name": "tag:Name",
                    "Values": [self.name],
                }
            ]
        )
        if len(response["Subnets"]) == 0:
            return None
        elif len(response["Subnets"]) == 1:
            return AWSSubnetConfig.from_response(response["Subnets"][0])
        else:
            raise ValueError(f"Found multiple subnets with name {self.name}.")

    def create(self) -> AWSSubnetConfig:
        ec2_resource.create_subnet(
            VpcId=self.vpc_id,
            CidrBlock=self.cidr_block,
            AvailabilityZone=self.availability_zone,
            TagSpecifications=[
                {
                    "ResourceType": "subnet",
                    "Tags": [
                        {
                            "Key": "Name",
                            "Value": self.name,
                        },
                    ],
                },
            ],
        )
        config = self.force_fetch_config()
        if self.route_table_id:
            logging.info(
                f"Associating route table ID {self.route_table_id} to {str(self)}..."
            )
            ec2_client.associate_route_table(
                RouteTableId=self.route_table_id, SubnetId=config.subnet_id
            )
        return config

    # User end security configuration modifications should not be reset
    def update(self, config: AWSSubnetConfig) -> AWSSubnetConfig:
        return config

    def destroy(self, config: AWSSubnetConfig) -> None:
        ec2_client.delete_subnet(SubnetId=config.subnet_id)


class AWSSubnetGroupConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        description: str,
        subnet_ids: List[str],
    ):
        super().__init__()
        self.name = name
        self.description = description
        self.subnet_ids = subnet_ids

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSSubnetGroupConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSSubnetGroupConfig]:
        try:
            response = rds_client.describe_db_subnet_groups(DBSubnetGroupName=self.name)
        except rds_client.exceptions.DBSubnetGroupNotFoundFault:
            return None
        if len(response["DBSubnetGroups"]) == 0:
            return None
        elif len(response["DBSubnetGroups"]) == 1:
            return AWSSubnetGroupConfig.from_response(response["DBSubnetGroups"][0])
        else:
            raise ValueError(f"Found multiple subnet groups with name {self.name}.")

    def create(self) -> AWSSubnetGroupConfig:
        return AWSSubnetGroupConfig.from_response(
            rds_client.create_db_subnet_group(
                DBSubnetGroupName=self.name,
                DBSubnetGroupDescription=self.description,
                SubnetIds=self.subnet_ids,
            )["DBSubnetGroup"]
        )

    # User end security configuration modifications should not be reset
    def update(self, config: AWSSubnetGroupConfig) -> AWSSubnetGroupConfig:
        return config

    def destroy(self, config: AWSSubnetGroupConfig) -> None:
        rds_client.delete_db_subnet_group(DBSubnetGroupName=self.name)


class AWSClusterSubnetGroupConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        description: str,
        subnet_ids: List[str],
    ):
        super().__init__()
        self.name = name
        self.description = description
        self.subnet_ids = subnet_ids

    # User end security configuration modifications should not be reset
    def requires_update(self, config: AWSClusterSubnetGroupConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSClusterSubnetGroupConfig]:
        try:
            response = redshift_client.describe_cluster_subnet_groups(
                ClusterSubnetGroupName=self.name
            )
        except redshift_client.exceptions.ClusterSubnetGroupNotFoundFault:
            return None
        if len(response["ClusterSubnetGroups"]) == 0:
            return None
        elif len(response["ClusterSubnetGroups"]) == 1:
            return AWSClusterSubnetGroupConfig.from_response(
                response["ClusterSubnetGroups"][0]
            )
        else:
            raise ValueError(
                f"Found multiple cluster subnet groups with name {self.name}."
            )

    def create(self) -> AWSClusterSubnetGroupConfig:
        return AWSClusterSubnetGroupConfig.from_response(
            redshift_client.create_cluster_subnet_group(
                ClusterSubnetGroupName=self.name,
                Description=self.description,
                SubnetIds=self.subnet_ids,
            )["ClusterSubnetGroup"]
        )

    # User end security configuration modifications should not be reset
    def update(
        self, config: AWSClusterSubnetGroupConfig
    ) -> AWSClusterSubnetGroupConfig:
        return config

    def destroy(self, config: AWSClusterSubnetGroupConfig) -> None:
        redshift_client.delete_cluster_subnet_group(ClusterSubnetGroupName=self.name)


class AWSRDSConnector(AWSResourceConnector):
    def __init__(
        self,
        identifier: str,
        engine: Optional[str] = None,
        db_name: Optional[str] = None,
        db_instance_class: str = "db.t3.micro",
        allocated_storage: int = 20,
        sg_id: Optional[str] = None,
        subnet_group_name: Optional[str] = None,
    ):
        super().__init__()
        self.identifier = identifier
        self.engine = engine
        self.db_name = db_name
        self.db_instance_class = db_instance_class
        self.allocated_storage = allocated_storage
        self.sg_id = sg_id
        self.subnet_group_name = subnet_group_name

    def requires_update(self, config: AWSRDSConfig) -> bool:
        if self.engine is not None and self.engine != config.engine:
            raise ValueError("Cannot change RDS engine with update.")
        if self.db_name is not None and self.db_name != config.db_name:
            raise ValueError("Cannot change RDS database name with update.")
        return (self.db_instance_class != config.db_instance_class) or (
            self.allocated_storage != config.allocated_storage
        )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.identifier}"

    def fetch_config(self) -> Optional[AWSRDSConfig]:
        try:
            response = rds_client.describe_db_instances(
                DBInstanceIdentifier=self.identifier
            )
        except rds_client.exceptions.DBInstanceNotFoundFault:
            return None
        if len(response["DBInstances"]) == 0:
            return None
        elif len(response["DBInstances"]) == 1:
            logging.info(f"Waiting on instance available state for '{self}'...")
            rds_db_instance_available_waiter.wait(DBInstanceIdentifier=self.identifier)
            return AWSRDSConfig.from_response(
                rds_client.describe_db_instances(DBInstanceIdentifier=self.identifier)[
                    "DBInstances"
                ][0]
            )
        else:
            raise ValueError(f"Found multiple RDS with ID {self.identifier}.")

    def update(self, config: AWSRDSConfig) -> AWSRDSConfig:
        rds_client.modify_db_instance(
            DBInstanceIdentifier=self.identifier,
            ApplyImmediately=True,
            MasterUserPassword=AWSRDSConfig.get_master_password(),
            DBInstanceClass=self.db_instance_class,
            AllocatedStorage=self.allocated_storage,
            PubliclyAccessible=True,
            **(
                {}
                if (self.sg_id is None) and (self.subnet_group_name is None)
                else {
                    "VpcSecurityGroupIds": [self.sg_id],
                    "DBSubnetGroupName": self.subnet_group_name,
                }
            ),
        )
        logging.info(f"Waiting on instance available state for '{self}'...")
        rds_db_instance_available_waiter.wait(DBInstanceIdentifier=self.identifier)
        return self.force_fetch_config()

    def create(self) -> AWSRDSConfig:
        rds_client.create_db_instance(
            DBInstanceIdentifier=self.identifier,
            Engine=self.engine,
            DBName=self.db_name,
            MasterUsername=AWSRDSConfig.get_master_username(),
            MasterUserPassword=AWSRDSConfig.get_master_password(),
            DBInstanceClass=self.db_instance_class,
            AllocatedStorage=self.allocated_storage,
            PubliclyAccessible=True,
            **(
                {}
                if (self.sg_id is None) and (self.subnet_group_name is None)
                else {
                    "VpcSecurityGroupIds": [self.sg_id],
                    "DBSubnetGroupName": self.subnet_group_name,
                }
            ),
        )
        logging.info(f"Waiting on instance available state for '{self}'...")
        rds_db_instance_available_waiter.wait(DBInstanceIdentifier=self.identifier)
        return self.force_fetch_config()

    def destroy(self, config: AWSRDSConfig) -> None:
        rds_client.delete_db_instance(
            DBInstanceIdentifier=self.identifier, SkipFinalSnapshot=True
        )
        logging.info(f"Waiting on instance deleted state for '{self}'...")
        rds_client.get_waiter("db_instance_deleted").wait(
            DBInstanceIdentifier=config.identifier
        )


class AWSSQSConnector(AWSResourceConnector):
    def __init__(self, name: str):
        self.name = name

    def requires_update(self, config: AWSResourceConfig) -> bool:
        return False

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def get_queue_url(self) -> Optional[str]:
        return sqs_client.get_queue_url(QueueName=self.name)["QueueUrl"]

    def fetch_config(self) -> Optional[AWSSQSConfig]:
        try:
            url = self.get_queue_url()
        except sqs_client.exceptions.QueueDoesNotExist:
            return None
        arn = sqs_client.get_queue_attributes(
            QueueUrl=url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        return AWSSQSConfig(
            response={},
            name=self.name,
            arn=arn,
            url=url,
        )

    def create(self) -> AWSSQSConfig:
        sqs_client.create_queue(
            QueueName=self.name, Attributes={"VisibilityTimeout": "43200"}
        )
        time.sleep(1)  # AWS docs say SQS needs one second to be ready
        return self.force_fetch_config()

    def update(self, config: AWSSQSConfig) -> AWSSQSConfig:
        return self.force_fetch_config()

    def destroy(self, config: AWSSQSConfig) -> None:
        sqs_client.delete_queue(QueueUrl=self.get_queue_url())


class AWSEventSourceMappingConnector(AWSResourceConnector):
    def __init__(self, source_arn: str, function_name: str):
        self.source_arn = source_arn
        self.function_name = function_name

    def requires_update(self, config: AWSEventSourceMappingConfig) -> bool:
        return True

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.source_arn}->{self.function_name}"

    def fetch_config(self) -> AWSEventSourceMappingConfig:
        event_source_mapping = None
        for event_source_page in lambda_client.get_paginator(
            "list_event_source_mappings"
        ).paginate(EventSourceArn=self.source_arn, FunctionName=self.function_name):
            for event_source in event_source_page["EventSourceMappings"]:
                event_source_mapping = event_source
        return (
            None
            if event_source_mapping is None
            else AWSEventSourceMappingConfig.from_response(event_source_mapping)
        )

    def create(self) -> AWSEventSourceMappingConfig:
        return AWSEventSourceMappingConfig.from_response(
            lambda_client.create_event_source_mapping(
                EventSourceArn=self.source_arn,
                FunctionName=self.function_name,
                BatchSize=1,
                Enabled=True,
            )
        )

    def update(
        self, config: AWSEventSourceMappingConfig
    ) -> AWSEventSourceMappingConfig:
        return AWSEventSourceMappingConfig.from_response(
            lambda_client.update_event_source_mapping(
                BatchSize=1,
                Enabled=True,
                FunctionName=self.function_name,
                UUID=config.uuid,
            )
        )

    def destroy(self, config: AWSEventSourceMappingConfig) -> None:
        lambda_client.delete_event_source_mapping(UUID=config.uuid)


class AWSLambdaConnector(AWSResourceConnector):
    def __init__(
        self,
        func_name: str,
        role_arn: Optional[str],
        code_config: Dict,
        handler: str,
        environ: Optional[Dict[str, str]] = None,
        timeout: int = 60,
        memory: int = 1024,
        security_group_id: Optional[str] = None,
        subnet_ids: Optional[List[str]] = None,
    ):
        super().__init__()
        self.func_name = func_name
        self.role_arn = role_arn
        self.code_config = code_config
        self.handler = handler
        self.environ = environ or {}
        self.timeout = timeout
        self.memory = memory
        self.security_group_id = security_group_id
        self.subnet_ids = subnet_ids or []

    # Leave as true, because you can't tell if code or environment has changed and update is quick
    def requires_update(self, config: AWSLambdaConfig) -> bool:
        return True

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.func_name}"

    def construct_params(self) -> Dict:
        return {
            "FunctionName": self.func_name,
            "Role": self.role_arn,
            "Handler": self.handler,
            "Timeout": self.timeout,
            "MemorySize": self.memory,
            "Runtime": f"python{sys.version_info.major}.{sys.version_info.minor}",
            "Environment": {
                "Variables": self.environ,
            },
            **(
                {}
                if (self.security_group_id is None) and (not self.subnet_ids)
                else {
                    "VpcConfig": {
                        "SubnetIds": self.subnet_ids,
                        "SecurityGroupIds": [self.security_group_id],
                    }
                }
            ),
        }

    def add_s3_permission(self, bucket) -> None:
        try:
            lambda_client.remove_permission(
                FunctionName=self.func_name,
                StatementId="s3",
            )
            logging.info(f"Deleted s3 permission for '{self.func_name}' lambda.")
        except lambda_client.exceptions.ResourceNotFoundException:
            logging.info(f"S3 permission for {self.func_name} lambda did not exist.")
        logging.info(f"Creating s3 permission for {self.func_name} lambda...")
        lambda_client.add_permission(
            Action="lambda:InvokeFunction",
            FunctionName=self.func_name,
            Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{bucket}",
            StatementId="s3",
        )

    def fetch_config(self) -> Optional[AWSLambdaConfig]:
        try:
            return AWSLambdaConfig.from_response(
                lambda_client.get_function(FunctionName=self.func_name)["Configuration"]
            )
        except lambda_client.exceptions.ResourceNotFoundException:
            return None

    def update(self, config: AWSLambdaConfig) -> AWSLambdaConfig:
        lambda_client.get_waiter("function_updated").wait(FunctionName=self.func_name)
        lambda_client.update_function_code(
            FunctionName=self.func_name, **self.code_config
        )
        lambda_client.get_waiter("function_updated").wait(FunctionName=self.func_name)
        return AWSLambdaConfig.from_response(
            lambda_client.update_function_configuration(**self.construct_params())
        )

    def create(self) -> AWSLambdaConfig:
        config = AWSLambdaConfig.from_response(
            lambda_client.create_function(
                **self.construct_params(), Code=self.code_config
            )
        )
        return config

    def destroy(self, config: AWSLambdaConfig) -> None:
        lambda_client.get_waiter("function_updated").wait(FunctionName=self.func_name)
        lambda_client.delete_function(FunctionName=self.func_name)


class AWSInstanceProfileConnector(AWSResourceConnector):
    def __init__(self, name: str, role_name: str):
        self.name = name
        self.role_name = role_name

    def requires_update(self, config: AWSInstanceProfileConfig) -> bool:
        return (
            (self.name != config.name)
            or ((self.role_name is not None) and (config.role is None))
            or (self.role_name != config.role.role_name)
        )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSInstanceProfileConfig]:
        try:
            return AWSInstanceProfileConfig.from_response(
                iam_client.get_instance_profile(InstanceProfileName=self.name)[
                    "InstanceProfile"
                ]
            )
        except iam_client.exceptions.NoSuchEntityException:
            return None

    def create(self) -> AWSInstanceProfileConfig:
        iam_client.create_instance_profile(InstanceProfileName=self.name)
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=self.name,
            RoleName=self.role_name,
        )
        iam_instance_profile_exists_waiter.wait(InstanceProfileName=self.name)
        return self.force_fetch_config()

    def update(self, config: AWSInstanceProfileConfig) -> AWSInstanceProfileConfig:
        if (self.role_name is not None) and (config.role is None):
            logging.info(f"'{self}' adding role '{self.role_name}'...")
            iam_client.add_role_to_instance_profile(
                InstanceProfileName=self.name,
                RoleName=self.role_name,
            )
            time.sleep(5)
        elif self.role_name != config.role.role_name:
            logging.info(
                f"'{self}' switching roles from '{config.role.role_name}' to '{self.role_name}'...",
            )
            iam_client.remove_role_from_instance_profile(
                InstanceProfileName=self.name, RoleName=config.role.role_name
            )
            iam_client.add_role_to_instance_profile(
                InstanceProfileName=self.name,
                RoleName=self.role_name,
            )
            time.sleep(5)
        else:
            logging.info(f"'{self}' already has the requested IAM role...")
        iam_instance_profile_exists_waiter.wait(InstanceProfileName=self.name)
        return self.force_fetch_config()

    def destroy(self, config: AWSInstanceProfileConfig) -> None:
        try:
            iam_client.remove_role_from_instance_profile(
                InstanceProfileName=self.name, RoleName=self.role_name
            )
        except iam_client.exceptions.NoSuchEntityException:
            logging.info(
                f"Instance profile '{self.name}' has already been stripped of role '{self.role_name}', skipping...",
            )
        iam_client.delete_instance_profile(InstanceProfileName=self.name)


class AWSEMRConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        log_uri: str,
        installer_uri: str,
        role_name: str,
        master_instance_type: Optional[str] = None,
        slave_instance_type: Optional[str] = None,
        instance_count: Optional[int] = None,
        step_concurrency: Optional[int] = None,
        environ: Optional[Dict] = None,
        master_sg_id: Optional[str] = None,
        slave_sg_id: Optional[str] = None,
        subnet_id: Optional[str] = None,
    ):
        super().__init__()
        self.name = name
        self.log_uri = log_uri
        self.installer_uri = installer_uri
        self.role_name = role_name
        self.master_instance_type = master_instance_type or "m5.xlarge"
        self.slave_instance_type = slave_instance_type or "m5.xlarge"
        self.instance_count = instance_count or 1
        self.step_concurrency = step_concurrency or 5
        self.environ = environ or {}
        self.master_sg_id = master_sg_id
        self.slave_sg_id = slave_sg_id
        self.subnet_id = subnet_id

    def requires_update(self, config: AWSEMRConfig) -> bool:
        return False

    @staticmethod
    def get_hadoop_password():
        return os.environ["EMR_PW"]

    @staticmethod
    def get_hadoop_username():
        return "hadoop"

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def get_key_pair_name(self):
        return f"{self.name}-key-pair"

    def fetch_config(self) -> Optional[AWSEMRConfig]:
        for cluster_page in emr_client.get_paginator("list_clusters").paginate():
            for cluster in cluster_page["Clusters"]:
                if cluster["Name"] == self.name:
                    if cluster["Status"]["State"] in ["RUNNING", "WAITING"]:
                        return AWSEMRConfig.from_response(
                            emr_client.describe_cluster(ClusterId=cluster["Id"])[
                                "Cluster"
                            ]
                        )
                    elif cluster["Status"]["State"] in ["STARTING", "BOOTSTRAPPING"]:
                        logging.info(
                            f"Waiting on cluster ready state for '{self}' as it is currently '{cluster['Status']['State']}'...",
                        )
                        emr_cluster_running_waiter.wait(ClusterId=cluster["Id"])
                        return AWSEMRConfig.from_response(
                            emr_client.describe_cluster(ClusterId=cluster["Id"])[
                                "Cluster"
                            ]
                        )
        return None

    @staticmethod
    def cluster_arn_to_id(arn: str) -> str:
        for cluster_page in emr_client.get_paginator("list_clusters").paginate():
            for cluster in cluster_page["Clusters"]:
                if cluster["ClusterArn"] == arn:
                    return cluster["Id"]
        raise ValueError(f"Could not find cluster with ARN '{arn}'")

    def create(self) -> AWSEMRConfig:
        try:
            response = ec2_client.create_key_pair(KeyName=self.get_key_pair_name())
            open(self.get_key_pair_name(), "w").write(response["KeyMaterial"])
        except ec2_client.exceptions.ClientError:
            logging.info(f"Key pair already exists for '{self}', reusing...")
        run_command(f"chmod 400 {self.get_key_pair_name()}")
        logging.info(f"Creating '{self}' cluster...")
        arn = emr_client.run_job_flow(
            Name=self.name,
            ReleaseLabel="emr-6.4.0",
            Applications=[{"Name": "spark"}],
            Instances={
                "MasterInstanceType": self.master_instance_type,
                "SlaveInstanceType": self.slave_instance_type,
                "InstanceCount": self.instance_count,
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
                "Ec2KeyName": self.get_key_pair_name(),
                **(
                    {}
                    if (self.master_sg_id is None)
                    and (self.slave_sg_id is None)
                    and (self.subnet_id is None)
                    else {
                        "EmrManagedMasterSecurityGroup": self.master_sg_id,
                        "EmrManagedSlaveSecurityGroup": self.slave_sg_id,
                        "Ec2SubnetId": self.subnet_id,
                    }
                ),
            },
            StepConcurrencyLevel=self.step_concurrency,
            VisibleToAllUsers=True,
            JobFlowRole=self.role_name,
            ServiceRole=self.role_name,
            LogUri=self.log_uri,
            BootstrapActions=[
                {
                    "Name": "setup-env",
                    "ScriptBootstrapAction": {"Path": self.installer_uri, "Args": []},
                }
            ],
            Configurations=[
                {
                    "Classification": "spark-env",
                    "Properties": {},
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": self.environ,
                        }
                    ],
                }
            ]
            if self.environ
            else None,
        )["ClusterArn"]
        logging.info(f"Waiting on cluster ready state for '{self}'...")
        emr_cluster_running_waiter.wait(ClusterId=self.cluster_arn_to_id(arn))
        return self.force_fetch_config()

    def update(self, config: AWSEMRConfig) -> AWSEMRConfig:
        emr_client.terminate_job_flows(JobFlowIds=[config.cluster_id])
        self.create()
        logging.info(f"Waiting on cluster ready state for '{self}'...")
        emr_cluster_running_waiter.wait(ClusterId=config.cluster_id)
        return self.force_fetch_config()

    def destroy(self, config: AWSEMRConfig) -> None:
        emr_client.terminate_job_flows(JobFlowIds=[config.cluster_id])
        ec2_client.delete_key_pair(KeyName=self.get_key_pair_name())
        try:
            run_command(f"rm {self.get_key_pair_name()} -f")
        except RuntimeError:
            logging.info(f"Skipped deletion of {self.get_key_pair_name()}")


class AWSEMRServerlessConnector(AWSResourceConnector):
    def __init__(
        self,
        name: str,
        release_label: str = "emr-6.6.0",
        initial_driver_worker_count: int = 1,
        initial_driver_cpu: str = "1vCPU",
        initial_driver_memory: str = "2GB",
        initial_executor_worker_count: int = 1,
        initial_executor_cpu: str = "1vCPU",
        initial_executor_memory: str = "2GB",
        max_cpu: Optional[str] = None,
        max_memory: Optional[str] = None,
        idle_timeout_minutes: int = 15,
        sg_id: Optional[str] = None,
        subnet_id: Optional[str] = None,
    ):
        self.name = name
        self.release_label = release_label
        self.initial_driver_worker_count = initial_driver_worker_count
        self.initial_driver_cpu = initial_driver_cpu
        self.initial_driver_memory = initial_driver_memory
        self.initial_executor_worker_count = initial_executor_worker_count
        self.initial_executor_cpu = initial_executor_cpu
        self.initial_executor_memory = initial_executor_memory
        self.max_cpu = max_cpu
        self.max_memory = max_memory
        self.idle_timeout_minutes = idle_timeout_minutes
        self.sg_id = sg_id
        self.subnet_id = subnet_id

    def requires_update(self, config: AWSEMRServerlessConfig) -> bool:
        if self.release_label != config.release_label:
            raise ValueError(
                f"Cannot change release label for {str(self)} with update ({config.release_label} -> {self.release_label})"
            )
        if (
            (self.initial_driver_worker_count != config.initial_driver_worker_count)
            or (self.initial_driver_cpu != config.initial_driver_cpu)
            or (self.initial_driver_memory != config.initial_driver_memory)
            or (
                self.initial_executor_worker_count
                != config.initial_executor_worker_count
            )
            or (self.initial_executor_cpu != config.initial_executor_cpu)
            or (self.initial_executor_memory != config.initial_executor_memory)
            or (self.max_cpu != config.max_cpu)
            or (self.max_memory != config.max_memory)
            or (self.idle_timeout_minutes != config.idle_timeout_minutes)
            # or (self.sg_id != config.sg_id)
            # or (self.subnet_id != config.subnet_id)
        ):
            return True
        return False

    def wait_on_application_ready(self) -> AWSEMRServerlessConfig:
        config = self.force_fetch_config()
        while config.state not in [
            "CREATED",
            "STARTING",
            "STARTED",
            "STOPPING",
            "STOPPED",
        ]:
            if config.state == "TERMINATED":
                raise ValueError(
                    f"Got 'TERMINATED' state while waiting on application ready for {str(self)}."
                )
            logging.info(f"Waiting on creation completion for {str(self)}...")
            logging.info(
                f"Current state : '{config.state}'. Application ID '{config.application_id}'."
            )
            time.sleep(10)
            config = self.fetch_config()
        return config

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.name}"

    def fetch_config(self) -> Optional[AWSEMRServerlessConfig]:
        for application_page in emr_serverless_client.get_paginator(
            "list_applications"
        ).paginate():
            for application in application_page["applications"]:
                if application["name"] == self.name:
                    return AWSEMRServerlessConfig.from_response(
                        emr_serverless_client.get_application(
                            applicationId=application["id"]
                        )["application"]
                    )
            return None

    def get_config_dict(self) -> Dict:
        return {
            "initialCapacity": {
                "DRIVER": {
                    "workerCount": self.initial_driver_worker_count,
                    "workerConfiguration": {
                        "cpu": self.initial_driver_cpu,
                        "memory": self.initial_driver_memory,
                    },
                },
                "EXECUTOR": {
                    "workerCount": self.initial_executor_worker_count,
                    "workerConfiguration": {
                        "cpu": self.initial_executor_cpu,
                        "memory": self.initial_executor_memory,
                    },
                },
            },
            "autoStopConfiguration": {
                "enabled": True,
                "idleTimeoutMinutes": self.idle_timeout_minutes,
            },
            "networkConfiguration": {
                "securityGroupIds": [self.sg_id] if self.sg_id else [],
                "subnetIds": [self.subnet_id] if self.subnet_id else [],
            },
            **(
                {
                    "maximumCapacity": {
                        "cpu": self.max_cpu,
                        "memory": self.max_memory,
                    }
                }
                if ((self.max_cpu is not None) and (self.max_memory is not None))
                else {}
            ),
        }

    def create(self) -> AWSEMRServerlessConfig:
        emr_serverless_client.create_application(
            name=self.name,
            releaseLabel=self.release_label,
            type="SPARK",
            **self.get_config_dict(),
        )
        return self.wait_on_application_ready()

    def update(self, config: AWSEMRServerlessConfig) -> AWSEMRServerlessConfig:
        emr_serverless_client.update_application(
            applicationId=config.application_id,
            **self.get_config_dict(),
        )
        return self.wait_on_application_ready()

    def destroy(self, config: AWSEMRServerlessConfig) -> None:
        # TODO : cancel all jobs and stop application
        emr_serverless_client.delete_application(applicationId=config.application_id)


class AWSRedshiftConnector(AWSResourceConnector):
    def __init__(
        self,
        identifier: str,
        role_arn: str,
        db_name: Optional[str] = None,
        number_of_nodes: Optional[int] = None,
        node_type: Optional[str] = None,
        sg_id: Optional[str] = None,
        cluster_subnet_group_name: Optional[str] = None,
    ):
        super().__init__()
        self.identifier = identifier
        self.role_arn = role_arn
        self.db_name = db_name or "adf"
        self.number_of_nodes = number_of_nodes or 1
        self.node_type = node_type or "ds2.xlarge"
        self.sg_id = sg_id
        self.cluster_subnet_group_name = cluster_subnet_group_name

    def requires_update(self, config: AWSRedshiftConfig) -> bool:
        if self.db_name != config.db_name:
            raise ValueError("Cannot change db name with update.")
        return (self.node_type != config.node_type) or (
            self.number_of_nodes != config.number_of_nodes
        )

    def __str__(self) -> str:
        return f"{self.__class__.__name__}:{self.identifier}"

    def fetch_config(self) -> Optional[AWSRedshiftConfig]:
        try:
            response = redshift_client.describe_clusters(
                ClusterIdentifier=self.identifier
            )
        except redshift_client.exceptions.ClusterNotFoundFault:
            return None
        if len(response["Clusters"]) == 0:
            return None
        elif len(response["Clusters"]) == 1:
            logging.info(f"Waiting on cluster available state for '{self}'...")
            redshift_available_waiter.wait(ClusterIdentifier=self.identifier)
            return AWSRedshiftConfig.from_response(
                redshift_client.describe_clusters(ClusterIdentifier=self.identifier)[
                    "Clusters"
                ][0]
            )
        else:
            raise ValueError(
                f"Found multiple redshift clusters with ID {self.identifier}."
            )

    def create(self) -> AWSRedshiftConfig:
        redshift_client.create_cluster(
            ClusterIdentifier=self.identifier,
            ClusterType="single-node" if self.number_of_nodes == 1 else "multi-node",
            NodeType=self.node_type,
            MasterUsername="redshift",
            MasterUserPassword=AWSRedshiftConfig.get_master_password(),
            DBName=self.db_name,
            PubliclyAccessible=True,
            IamRoles=[self.role_arn],
            **(
                {
                    "NumberOfNodes": self.number_of_nodes,
                }
                if self.number_of_nodes != 1
                else {}
            ),
            **(
                {}
                if (self.sg_id is None) and (self.cluster_subnet_group_name is None)
                else {
                    "ClusterSubnetGroupName": self.cluster_subnet_group_name,
                    "VpcSecurityGroupIds": [self.sg_id],
                }
            ),
        )
        logging.info(f"Waiting on cluster available state for '{self}'...")
        redshift_available_waiter.wait(ClusterIdentifier=self.identifier)
        return self.force_fetch_config()

    def update(self, config: AWSRedshiftConfig) -> AWSRedshiftConfig:
        redshift_client.modify_cluster(
            ClusterIdentifier=self.identifier,
            MasterUserPassword=AWSRDSConfig.get_master_password(),
            ClusterType="single-node" if self.number_of_nodes == 1 else "multi-node",
            NodeType=self.node_type,
            **(
                {
                    "NumberOfNodes": self.number_of_nodes,
                }
                if self.number_of_nodes != 1
                else {}
            ),
            **({} if (self.sg_id is None) else {"VpcSecurityGroupIds": [self.sg_id]}),
        )
        logging.info(f"Waiting on cluster available state for '{self}'...")
        redshift_available_waiter.wait(ClusterIdentifier=self.identifier)
        return self.force_fetch_config()

    def destroy(self, config: AWSRedshiftConfig) -> None:
        redshift_client.delete_cluster(
            ClusterIdentifier=self.identifier,
            SkipFinalClusterSnapshot=True,
        )

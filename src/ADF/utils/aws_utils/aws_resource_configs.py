import os

from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Any
from typing_extensions import Self


def first_or_none(l: List) -> Optional[Any]:
    return l[0] if l else None


class AWSResourceConfig(ABC):
    def __init__(self, response: Dict):
        self.response = response

    @classmethod
    @abstractmethod
    def from_response(cls, response: Dict) -> Self:
        pass


class AWSIAMRoleConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        arn: str,
        role_id: str,
        role_name: str,
        path: str,
    ):
        super().__init__(response)
        self.arn = arn
        self.role_id = role_id
        self.role_name = role_name
        self.path = path

    @classmethod
    def from_response(cls, response: Dict) -> "AWSIAMRoleConfig":
        return cls(
            response=response,
            arn=response["Arn"],
            role_id=response["RoleId"],
            role_name=response["RoleName"],
            path=response["Path"],
        )


class AWSVPCConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        name: str,
        vpc_id: str,
        cidr_block: str,
    ):
        super().__init__(response)
        self.name = name
        self.vpc_id = vpc_id
        self.cidr_block = cidr_block

    @classmethod
    def from_response(cls, response: Dict) -> "AWSVPCConfig":
        return cls(
            response=response,
            name={tag["Key"]: tag["Value"] for tag in response["Tags"]}.get(
                "Name", None
            ),
            vpc_id=response["VpcId"],
            cidr_block=response["CidrBlock"],
        )


class AWSInternetGatewayConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        name: str,
        internet_gateway_id: str,
    ):
        super().__init__(response)
        self.name = name
        self.internet_gateway_id = internet_gateway_id

    @classmethod
    def from_response(cls, response: Dict) -> "AWSInternetGatewayConfig":
        return cls(
            response=response,
            name={tag["Key"]: tag["Value"] for tag in response["Tags"]}.get(
                "Name", None
            ),
            internet_gateway_id=response["InternetGatewayId"],
        )


class AWSNATGatewayConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        name: str,
        nat_gateway_id: str,
        subnet_id: str,
    ):
        super().__init__(response)
        self.name = name
        self.nat_gateway_id = nat_gateway_id
        self.subnet_id = subnet_id

    @classmethod
    def from_response(cls, response: Dict) -> "AWSNATGatewayConfig":
        return cls(
            response=response,
            name={tag["Key"]: tag["Value"] for tag in response["Tags"]}.get(
                "Name", None
            ),
            nat_gateway_id=response["NatGatewayId"],
            subnet_id=response["SubnetId"],
        )


class AWSVPCEndpointConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        name: str,
        vpc_endpoint_id: str,
        service_name: str,
        route_table_ids: List[str],
        subnet_ids: List[str],
    ):
        super().__init__(response)
        self.name = name
        self.vpc_endpoint_id = vpc_endpoint_id
        self.service_name = service_name
        self.route_table_ids = route_table_ids
        self.subnet_ids = subnet_ids

    @classmethod
    def from_response(cls, response: Dict) -> "AWSVPCEndpointConfig":
        return cls(
            response=response,
            name={tag["Key"]: tag["Value"] for tag in response["Tags"]}.get(
                "Name", None
            ),
            vpc_endpoint_id=response["VpcEndpointId"],
            service_name=response["ServiceName"],
            route_table_ids=response["RouteTableIds"],
            subnet_ids=response["SubnetIds"],
        )


class AWSElasticIPConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        name: str,
        allocation_id: str,
        public_ip: str,
        domain: str,
    ):
        super().__init__(response)
        self.name = name
        self.allocation_id = allocation_id
        self.public_ip = public_ip
        self.domain = domain

    @classmethod
    def from_response(cls, response: Dict) -> "AWSElasticIPConfig":
        return cls(
            response=response,
            name={tag["Key"]: tag["Value"] for tag in response["Tags"]}.get(
                "Name", None
            ),
            allocation_id=response["AllocationId"],
            public_ip=response["PublicIp"],
            domain=response["Domain"],
        )


class AWSRouteTableConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        name: str,
        route_table_id: str,
        vpc_id: str,
    ):
        super().__init__(response)
        self.name = name
        self.route_table_id = route_table_id
        self.vpc_id = vpc_id

    @classmethod
    def from_response(cls, response: Dict) -> "AWSRouteTableConfig":
        return cls(
            response=response,
            name={tag["Key"]: tag["Value"] for tag in response["Tags"]}.get(
                "Name", None
            ),
            route_table_id=response["RouteTableId"],
            vpc_id=response["VpcId"],
        )


class AWSSecurityGroupConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        group_id: str,
        vpc_id: str,
        group_name: str,
    ):
        super().__init__(response)
        self.group_id = group_id
        self.vpc_id = vpc_id
        self.group_name = group_name

    @classmethod
    def from_response(cls, response: Dict) -> "AWSSecurityGroupConfig":
        return cls(
            response=response,
            group_id=response["GroupId"],
            vpc_id=response["VpcId"],
            group_name=response["GroupName"],
        )


class AWSSubnetConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        name: str,
        subnet_id: str,
        cidr_block: str,
        availability_zone: str,
        availability_zone_id: str,
    ):
        super().__init__(response)
        self.name = name
        self.subnet_id = subnet_id
        self.cidr_block = cidr_block
        self.availability_zone = availability_zone
        self.availability_zone_id = availability_zone_id

    @classmethod
    def from_response(cls, response: Dict) -> "AWSSubnetConfig":
        return cls(
            response=response,
            name={tag["Key"]: tag["Value"] for tag in response["Tags"]}.get(
                "Name", None
            ),
            subnet_id=response["SubnetId"],
            cidr_block=response["CidrBlock"],
            availability_zone=response["AvailabilityZone"],
            availability_zone_id=response["AvailabilityZoneId"],
        )


class AWSSubnetGroupConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        arn: str,
        name: str,
        vpc_id: str,
        description: str,
        subnet_ids: List[str],
    ):
        super().__init__(response)
        self.arn = arn
        self.name = name
        self.vpc_id = vpc_id
        self.description = description
        self.subnet_ids = subnet_ids

    @classmethod
    def from_response(cls, response: Dict) -> "AWSSubnetGroupConfig":
        return cls(
            response=response,
            arn=response["DBSubnetGroupArn"],
            name=response["DBSubnetGroupName"],
            vpc_id=response["VpcId"],
            description=response["DBSubnetGroupDescription"],
            subnet_ids=[subnet["SubnetIdentifier"] for subnet in response["Subnets"]],
        )


class AWSClusterSubnetGroupConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        name: str,
        vpc_id: str,
        description: str,
        subnet_ids: List[str],
    ):
        super().__init__(response)
        self.name = name
        self.vpc_id = vpc_id
        self.description = description
        self.subnet_ids = subnet_ids

    @classmethod
    def from_response(cls, response: Dict) -> "AWSClusterSubnetGroupConfig":
        return cls(
            response=response,
            name=response["ClusterSubnetGroupName"],
            vpc_id=response["VpcId"],
            description=response["Description"],
            subnet_ids=[subnet["SubnetIdentifier"] for subnet in response["Subnets"]],
        )


class AWSInstanceProfileConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        arn: str,
        instance_profil_id: str,
        name: str,
        path: str,
        role: Optional[AWSIAMRoleConfig],
    ):
        super().__init__(response)
        self.arn = arn
        self.instance_profil_id = instance_profil_id
        self.name = name
        self.path = path
        self.role = role

    @classmethod
    def from_response(cls, response: Dict) -> "AWSInstanceProfileConfig":
        return cls(
            response=response,
            arn=response["Arn"],
            instance_profil_id=response["InstanceProfileId"],
            name=response["InstanceProfileName"],
            path=response["Path"],
            role=AWSIAMRoleConfig.from_response(response["Roles"][0])
            if len(response.get("Roles", []))
            else None,
        )


class AWSRDSConfig(AWSResourceConfig):
    engine_map = {"postgres": "postgresql"}

    @staticmethod
    def get_master_password() -> Optional[str]:
        return os.environ.get("RDS_PW")

    @staticmethod
    def get_master_username():
        return "adf_user"

    def __init__(
        self,
        response: Dict,
        identifier: str,
        db_instance_class: str,
        allocated_storage: int,
        engine: str,
        endpoint: str,
        port: int,
        master_user: str,
        db_name: str,
        status: str,
    ):
        super().__init__(response)
        self.identifier = identifier
        self.db_instance_class = db_instance_class
        self.allocated_storage = allocated_storage
        self.engine = engine
        self.endpoint = endpoint
        self.port = port
        self.master_user = master_user
        self.db_name = db_name
        self.status = status

    @classmethod
    def from_response(cls, response: Dict) -> "AWSRDSConfig":
        return cls(
            response=response,
            identifier=response["DBInstanceIdentifier"],
            db_instance_class=response["DBInstanceClass"],
            allocated_storage=response["AllocatedStorage"],
            engine=response["Engine"],
            endpoint=response["Endpoint"]["Address"],
            port=response["Endpoint"]["Port"],
            master_user=response["MasterUsername"],
            db_name=response["DBName"],
            status=response["DBInstanceStatus"],
        )

    def get_connection_string(self) -> str:
        if self.get_master_password() is None:
            raise ValueError(
                "Cannot construct connection string when RDS_PW is not set."
            )
        if self.engine not in self.engine_map:
            raise ValueError(
                f"Unhandled RDS engine '{self.engine}', cannot construct connection string."
            )
        if self.engine in self.engine_map:
            return f"{self.engine_map[self.engine]}://{self.master_user}:{self.get_master_password()}@{self.endpoint}:{self.port}/{self.db_name}"
        else:
            raise ValueError(f"Unhandled RDS engine {self.engine}.")


class AWSSQSConfig(AWSResourceConfig):
    def __init__(self, response: Dict, name: str, arn: str, url: str):
        super().__init__(response)
        self.name = name
        self.arn = arn
        self.url = url

    @classmethod
    def from_response(cls, response: Dict) -> "AWSSQSConfig":
        raise ValueError("SQS config can't be fetched from a single response")


class AWSEventSourceMappingConfig(AWSResourceConfig):
    def __init__(self, response: Dict, uuid: str, source_arn: str, function_arn: str):
        super().__init__(response)
        self.uuid = uuid
        self.source_arn = source_arn
        self.function_arn = function_arn

    @classmethod
    def from_response(cls, response: Dict) -> "AWSEventSourceMappingConfig":
        return cls(
            response=response,
            uuid=response["UUID"],
            source_arn=response["EventSourceArn"],
            function_arn=response["FunctionArn"],
        )


class AWSLambdaConfig(AWSResourceConfig):
    def __init__(self, response: Dict, name: str, arn: str):
        super().__init__(response)
        self.name = name
        self.arn = arn

    @classmethod
    def from_response(cls, response: Dict) -> "AWSLambdaConfig":
        return cls(
            response=response,
            name=response["FunctionName"],
            arn=response["FunctionArn"],
        )


class AWSEMRConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        cluster_id: str,
        cluster_arn: str,
        name: str,
        public_dns: str,
        log_uri: str,
        step_concurrency: Optional[int] = None,
    ):
        super().__init__(response)
        self.cluster_id = cluster_id
        self.cluster_arn = cluster_arn
        self.name = name
        self.public_dns = public_dns
        self.log_uri = log_uri
        self.step_concurrency = step_concurrency

    def get_key_pair_name(self):
        return f"{self.name}-key-pair"

    @classmethod
    def from_response(cls, response: Dict) -> "AWSEMRConfig":
        return AWSEMRConfig(
            response=response,
            cluster_id=response["Id"],
            cluster_arn=response["ClusterArn"],
            name=response["Name"],
            public_dns=response["MasterPublicDnsName"],
            log_uri=response.get("LogUri"),
            step_concurrency=response["StepConcurrencyLevel"],
        )


class AWSEMRServerlessConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        application_id: str,
        name: str,
        arn: str,
        state: str,
        application_type: str,
        release_label: str,
        initial_driver_worker_count: int,
        initial_driver_cpu: str,
        initial_driver_memory: str,
        initial_driver_disk: str,
        initial_executor_worker_count: int,
        initial_executor_cpu: str,
        initial_executor_memory: str,
        initial_executor_disk: str,
        max_cpu: Optional[str],
        max_memory: Optional[str],
        idle_timeout_minutes: int,
        sg_id: Optional[str],
        subnet_id: Optional[str],
    ):
        super().__init__(response)
        self.application_id = application_id
        self.name = name
        self.arn = arn
        self.state = state
        self.application_type = application_type
        self.release_label = release_label
        self.initial_driver_worker_count = initial_driver_worker_count
        self.initial_driver_cpu = initial_driver_cpu.replace(" ", "")
        self.initial_driver_memory = initial_driver_memory.replace(" ", "")
        self.initial_driver_disk = initial_driver_disk.replace(" ", "")
        self.initial_executor_worker_count = initial_executor_worker_count
        self.initial_executor_cpu = initial_executor_cpu.replace(" ", "")
        self.initial_executor_memory = initial_executor_memory.replace(" ", "")
        self.initial_executor_disk = initial_executor_disk.replace(" ", "")
        self.max_cpu = max_cpu.replace(" ", "") if max_cpu is not None else None
        self.max_memory = (
            max_memory.replace(" ", "") if max_memory is not None else None
        )
        self.idle_timeout_minutes = idle_timeout_minutes
        self.sg_id = sg_id
        self.subnet_id = subnet_id

    @classmethod
    def from_response(cls, response: Dict) -> "AWSEMRServerlessConfig":
        return cls(
            response=response,
            application_id=response["applicationId"],
            name=response["name"],
            arn=response["arn"],
            state=response["state"],
            application_type=response["type"],
            release_label=response["releaseLabel"],
            initial_driver_worker_count=response["initialCapacity"]["Driver"][
                "workerCount"
            ],
            initial_driver_cpu=response["initialCapacity"]["Driver"][
                "workerConfiguration"
            ]["cpu"],
            initial_driver_memory=response["initialCapacity"]["Driver"][
                "workerConfiguration"
            ]["memory"],
            initial_driver_disk=response["initialCapacity"]["Driver"][
                "workerConfiguration"
            ]["disk"],
            initial_executor_worker_count=response["initialCapacity"]["Executor"][
                "workerCount"
            ],
            initial_executor_cpu=response["initialCapacity"]["Executor"][
                "workerConfiguration"
            ]["cpu"],
            initial_executor_memory=response["initialCapacity"]["Executor"][
                "workerConfiguration"
            ]["memory"],
            initial_executor_disk=response["initialCapacity"]["Executor"][
                "workerConfiguration"
            ]["disk"],
            max_cpu=response["maximumCapacity"]["cpu"]
            if "maximumCapacity" in response
            else None,
            max_memory=response["maximumCapacity"]["memory"]
            if "maximumCapacity" in response
            else None,
            idle_timeout_minutes=response["autoStopConfiguration"][
                "idleTimeoutMinutes"
            ],
            sg_id=first_or_none(response["networkConfiguration"]["securityGroupIds"])
            if "networkConfiguration" in response
            else None,
            subnet_id=first_or_none(response["networkConfiguration"]["subnetIds"])
            if "networkConfiguration" in response
            else None,
        )


class AWSRedshiftConfig(AWSResourceConfig):
    def __init__(
        self,
        response: Dict,
        identifier: str,
        node_type: str,
        number_of_nodes: int,
        master_user: str,
        db_name: str,
        endpoint: str,
        port: int,
        publicly_accessible: str,
    ):
        super().__init__(response)
        self.identifier = identifier
        self.node_type = node_type
        self.number_of_nodes = number_of_nodes
        self.master_user = master_user
        self.db_name = db_name
        self.endpoint = endpoint
        self.port = port
        self.publicly_accessible = publicly_accessible

    @staticmethod
    def get_master_password() -> Optional[str]:
        return os.environ.get("REDSHIFT_PW")

    @classmethod
    def from_response(cls, response: Dict) -> "AWSRedshiftConfig":
        return cls(
            response=response,
            identifier=response["ClusterIdentifier"],
            node_type=response["NodeType"],
            number_of_nodes=response["NumberOfNodes"],
            master_user=response["MasterUsername"],
            db_name=response["DBName"],
            endpoint=response["Endpoint"]["Address"],
            port=response["Endpoint"]["Port"],
            publicly_accessible=response["PubliclyAccessible"],
        )

    def get_connection_string(self) -> str:
        if self.get_master_password() is None:
            raise ValueError(
                "Cannot construct connection string when RDS_PW is not set."
            )
        return f"redshift+psycopg2://{self.master_user}:{self.get_master_password()}@{self.endpoint}:{self.port}/{self.db_name}"

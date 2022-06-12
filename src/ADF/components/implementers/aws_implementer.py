import os
import logging

from abc import ABC
from io import BytesIO
from typing import Optional, Dict, Union
from pathlib import Path

from zipfile import ZipFile, ZIP_DEFLATED

from ADF.exceptions import AWSUnmanagedOperation
from ADF.config import ADFGlobalConfig
from ADF.components.implementers import ADFImplementer
from ADF.components.flow_config import ADFStep, ADFCollection
from ADF.components.layers import (
    AWSLambdaLayer,
    ManagedAWSLambdaLayer,
    PrebuiltAWSLambdaLayer,
    AWSEMRLayer,
    ManagedAWSEMRLayer,
    PrebuiltAWSEMRLayer,
    AWSRedshiftLayer,
    ManagedAWSRedshiftLayer,
    PrebuiltAWSRedshiftLayer,
)
from ADF.components.layer_transitions import (
    LambdaToEMRTransition,
    EMRToEMRTransition,
    EMRToRedshiftTransition,
    SameHostSQLToSQL,
)
from ADF.components.state_handlers import SQLStateHandler
from ADF.utils import (
    ec2_client,
    s3_client,
    lambda_client,
    s3_delete_prefix,
    zip_files,
    run_command,
    AWSIAMRoleConnector,
    AWSRDSConnector,
    AWSRDSConfig,
    AWSRedshiftConfig,
    AWSInstanceProfileConnector,
    AWSVPCConnector,
    AWSVPCConfig,
    AWSRouteTableConnector,
    AWSRouteTableConfig,
    AWSInternetGatewayConnector,
    AWSInternetGatewayConfig,
    AWSSecurityGroupConnector,
    AWSSubnetConnector,
    AWSSubnetGroupConnector,
    AWSSubnetGroupConfig,
    AWSClusterSubnetGroupConnector,
    AWSClusterSubnetGroupConfig,
)


class AWSImplementer(ADFImplementer, ABC):
    def __init__(
        self,
        name: str,
        log_folder: str,
        bucket: str,
        s3_prefix: str,
        state_handler_url: Optional[str],
        layers: Dict[str, Union[AWSLambdaLayer, AWSEMRLayer, AWSRedshiftLayer]],
    ):
        if (not name.isalpha()) or (name.lower() != name):
            raise ValueError(
                f"Implementer name must contain only lowercase letters, not '{name}'."
            )
        self.name = name
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        self.s3_icp = f"{s3_prefix}icp.yaml"
        self.s3_fcp_template = f"{s3_prefix}fcp.{{collection_name}}.yaml"
        self.s3_bootstrap_emr = f"{s3_prefix}bootstrap_emr.sh"
        self.local_lambda_zip_path = "lambda_package.zip"
        self.s3_lambda_zip_key = f"{self.s3_prefix}{self.local_lambda_zip_path}"
        self.local_emr_zip_path = "emr_package.zip"
        self.s3_emr_zip_key = f"{self.s3_prefix}{self.local_emr_zip_path}"
        self.log_folder = log_folder
        super().__init__(
            layers=layers,
            state_handler=SQLStateHandler(state_handler_url),
            transitions=[
                LambdaToEMRTransition,
                EMRToEMRTransition,
                EMRToRedshiftTransition,
                SameHostSQLToSQL,
            ],
        )

    def get_layer_s3_prefix(self, layer: str) -> str:
        return f"{self.s3_prefix}data_layers/{layer}/"

    @classmethod
    def from_class_config(cls, config: Dict) -> "AWSImplementer":
        mode = config["mode"]
        del config["mode"]
        aws_region_key = "AWS_REGION"
        if aws_region_key in config:
            ADFGlobalConfig.AWS_REGION = config[aws_region_key]
            del config[aws_region_key]
        if mode == "managed":
            return ManagedAWSImplementer(
                **config,
            )
        elif mode == "prebuilt":
            return PrebuiltAWSImplementer(
                **config,
            )
        else:
            raise ValueError(f"Unknown AWS config mode '{config['mode']}'.")

    def zip_emr(self):
        logging.info("Downloading EMR package...")
        s3_client.download_file(
            Bucket=ADFGlobalConfig.AWS_PACKAGE_BUCKET,
            Key=ADFGlobalConfig.AWS_EMR_PACKAGE_KEY,
            Filename=self.local_emr_zip_path,
        )
        for extra_package in self.extra_packages:
            logging.info(f"Adding code from {extra_package} in emr zip...")
            zip_handler = ZipFile(self.local_emr_zip_path, "a", ZIP_DEFLATED)
            zip_files(
                zip_hanlder=zip_handler,
                path=extra_package,
                prefix=f"extra_packages/{os.path.basename(os.path.normpath(extra_package))}/",
                exclude=lambda x: x.endswith(".zip"),
            )
            zip_handler.close()
        logging.info("Uploading emr code...")
        s3_client.upload_file(
            Filename=self.local_emr_zip_path,
            Bucket=self.bucket,
            Key=self.s3_emr_zip_key,
        )

    def zip_lambda(self):
        logging.info("Downloading Lambda package...")
        s3_client.download_file(
            Bucket=ADFGlobalConfig.AWS_PACKAGE_BUCKET,
            Key=ADFGlobalConfig.AWS_LAMBDA_PACKAGE_KEY,
            Filename=self.local_lambda_zip_path,
        )
        for extra_package in self.extra_packages:
            logging.info(f"Adding code from {extra_package} in lambda zip...")
            zip_handler = ZipFile(self.local_lambda_zip_path, "a", ZIP_DEFLATED)
            zip_files(
                zip_hanlder=zip_handler,
                path=os.path.join(extra_package, "src"),
            )
            zip_handler.close()
        logging.info("Uploading lambda code...")
        s3_client.upload_file(
            Filename=self.local_lambda_zip_path,
            Bucket=self.bucket,
            Key=self.s3_lambda_zip_key,
        )

    def update_code(self):
        logging.info(f"EXTRA PACKAGES : {self.extra_packages}")
        for extra_package in self.extra_packages:
            logging.info(f"Reinstalling package '{extra_package}' locally...")
            run_command("python3 setup.py install", cwd=extra_package)
        self.zip_emr()
        logging.info("Reinstalling EMR code...")
        for layer in self.layers.values():
            if isinstance(layer, AWSEMRLayer):
                emr_config = layer.emr_config
                cmds = [
                    "sudo rm -rf /home/hadoop/adf/* /home/hadoop/adf/.[!.]*",
                    f"aws s3 cp s3://{self.bucket}/{self.s3_emr_zip_key} /home/hadoop/adf/adf.zip",
                    "unzip /home/hadoop/adf/adf.zip -d /home/hadoop/adf/",
                ]
                for extra_package in self.extra_packages:
                    extra_package_name = os.path.basename(
                        os.path.normpath(extra_package)
                    )
                    cmds.append(
                        f"cd /home/hadoop/adf/extra_packages/{extra_package_name} ; sudo python3 setup.py install",
                    )
                for cmd in cmds:
                    run_command(
                        f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {layer.emr_config.get_key_pair_name()} hadoop@{emr_config.public_dns} '{cmd}'"
                    )
        self.zip_lambda()
        logging.info("Reinstalling Lambda code...")
        for layer in self.layers.values():
            if isinstance(layer, AWSLambdaLayer):
                lambda_config = layer.lambda_config
                lambda_client.get_waiter("function_updated").wait(
                    FunctionName=lambda_config.name
                )
                lambda_client.update_function_code(
                    FunctionName=lambda_config.name,
                    S3Bucket=self.bucket,
                    S3Key=self.s3_lambda_zip_key,
                )

    def setup_implementer_flows(self, flows: ADFCollection, icp: str, fcp: str):
        logging.info("Uploading implementer config to s3...")
        s3_client.upload_file(Filename=icp, Bucket=self.bucket, Key=self.s3_icp)
        logging.info("Uploading flow config to s3...")
        s3_client.upload_file(
            Filename=fcp,
            Bucket=self.bucket,
            Key=self.s3_fcp_template.format(collection_name=flows.name),
        )
        for layer in self.layers.values():
            if isinstance(layer, AWSEMRLayer):
                logging.info(
                    f"Uploading flow config to EMR cluster {layer.emr_config.name}...",
                )
                run_command(
                    f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {layer.emr_config.get_key_pair_name()} {icp} hadoop@{layer.emr_config.public_dns}:/home/hadoop/implementer.yaml"
                )
                run_command(
                    f"scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {layer.emr_config.get_key_pair_name()} {fcp} hadoop@{layer.emr_config.public_dns}:/home/hadoop/flows.{flows.name}.yaml"
                )

    def get_log_path(self, step: ADFStep, batch_id: str) -> str:
        return os.path.join(self.log_folder, f"{str(step)}.{batch_id}.log")

    def get_err_path(self, step: ADFStep, batch_id: str) -> str:
        return os.path.join(self.log_folder, f"{str(step)}.{batch_id}.err")


class ManagedAWSImplementer(AWSImplementer):
    def __init__(
        self,
        name: str,
        log_folder: str,
        bucket: str,
        s3_prefix: str,
        state_handler: Dict[str, str],
        lambda_layers: Dict[str, Dict[str, str]],
        emr_layers: Dict[str, Dict[str, str]],
        redshift_layers: Dict[str, Dict[str, str]],
    ):
        if (not name.isalpha()) or (name.lower() != name):
            raise ValueError(
                f"Implementer name must contain only lowercase letters, not '{name}'."
            )
        self.name = name
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        self.s3_icp = f"{s3_prefix}icp.yaml"
        self.s3_fcp_template = f"{s3_prefix}fcp.{{collection_name}}.yaml"
        self.s3_bootstrap_emr = f"{s3_prefix}bootstrap_emr.sh"
        self.local_lambda_zip_path = "lambda_package.zip"
        self.s3_lambda_zip_key = f"{self.s3_prefix}{self.local_lambda_zip_path}"
        self.local_emr_zip_path = "emr_package.zip"
        self.s3_emr_zip_key = f"{self.s3_prefix}{self.local_emr_zip_path}"
        self.log_folder = log_folder
        self.vpc_connector = AWSVPCConnector(
            name=f"{name}_vpc", cidr_block="10.0.0.0/16"
        )
        vpc_config: AWSVPCConfig = self.vpc_connector.update_or_create()
        self.internet_gateway_connector = AWSInternetGatewayConnector(
            name=f"{self.name}_ig", vpc_id=vpc_config.vpc_id
        )
        internet_gateway_config: AWSInternetGatewayConfig = (
            self.internet_gateway_connector.update_or_create()
        )
        self.route_table_connector = AWSRouteTableConnector(
            name=f"{self.name}_rt",
            vpc_id=vpc_config.vpc_id,
        )
        route_table_config: AWSRouteTableConfig = (
            self.route_table_connector.update_or_create()
        )
        all_layers = (
            list(lambda_layers.keys())
            + list(emr_layers.keys())
            + list(redshift_layers.keys())
        )
        self.iam_role_connectors: Dict[str, AWSIAMRoleConnector] = {
            layer: AWSIAMRoleConnector(role_name=f"{self.name}_{layer}_role")
            for layer in all_layers
        }
        for layer in all_layers:
            self.iam_role_connectors[layer].update_or_create()
        self.instance_profile_connectors: Dict[str, AWSInstanceProfileConnector] = {
            layer: AWSInstanceProfileConnector(
                name=self.iam_role_connectors[layer].update_or_create().role_name,
                role_name=self.iam_role_connectors[layer].update_or_create().role_name,
            )
            for layer in emr_layers
        }
        for layer in emr_layers:
            self.instance_profile_connectors[layer].update_or_create()
        self.subnet_connectors = [
            AWSSubnetConnector(
                name=f"{self.name}_{zone['ZoneName']}_zone_subnet",
                vpc_id=vpc_config.vpc_id,
                cidr_block=f"10.0.{n * 16}.0/20",
                availability_zone=zone["ZoneName"],
                route_table_id=route_table_config.route_table_id,
            )
            for n, zone in enumerate(
                ec2_client.describe_availability_zones()["AvailabilityZones"]
            )
        ]
        logging.info(
            f"Creating route to internet gateway {internet_gateway_config.name}...",
        )
        try:
            ec2_client.create_route(
                RouteTableId=route_table_config.route_table_id,
                GatewayId=internet_gateway_config.internet_gateway_id,
                DestinationCidrBlock="0.0.0.0/0",
            )
            logging.info(f"Route created for {internet_gateway_config.name} !")
        except ec2_client.exceptions.ClientError as e:
            if "RouteAlreadyExists" in str(e):
                logging.info(
                    f"Skipping route creation for {internet_gateway_config.name}...",
                )
            else:
                raise e
        subnet_ids = [
            subnet_connector.update_or_create().subnet_id
            for subnet_connector in self.subnet_connectors
        ]
        self.subnet_group_connector = AWSSubnetGroupConnector(
            name=f"{self.name}_subnet_group",
            description=f"{self.name} subnet group",
            subnet_ids=subnet_ids,
        )
        subnet_group_config: AWSSubnetGroupConfig = (
            self.subnet_group_connector.update_or_create()
        )
        self.cluster_subnet_group_connector = AWSClusterSubnetGroupConnector(
            name=f"{self.name}-cluster-subnet-group",
            description=f"{self.name} cluster subnet group",
            subnet_ids=subnet_ids,
        )
        cluster_subnet_group_config: AWSClusterSubnetGroupConfig = (
            self.cluster_subnet_group_connector.update_or_create()
        )
        self.state_handler_sg_connector = AWSSecurityGroupConnector(
            vpc_id=vpc_config.vpc_id,
            group_name=f"{self.name}_state_handler_sg",
            description=f"{self.name} state handler security group",
        )
        self.layer_sg_connectors: Dict[str, Dict[str, AWSSecurityGroupConnector]] = {
            "lambda": {},
            "emr": {},
            "redshift": {},
        }
        for layer in lambda_layers:
            self.layer_sg_connectors["lambda"][layer] = AWSSecurityGroupConnector(
                vpc_id=vpc_config.vpc_id,
                group_name=f"{self.name}_{layer}_lambda_sg",
                description=f"{self.name} {layer} lambda security group",
            )
        for layer in emr_layers:
            self.layer_sg_connectors["emr"][
                f"{layer}_master"
            ] = AWSSecurityGroupConnector(
                vpc_id=vpc_config.vpc_id,
                group_name=f"{self.name}_{layer}_emr_master_sg",
                description=f"{self.name} {layer} EMR master security group",
                init_ingress_rule="all_ssh",
            )
            self.layer_sg_connectors["emr"][
                f"{layer}_slave"
            ] = AWSSecurityGroupConnector(
                vpc_id=vpc_config.vpc_id,
                group_name=f"{self.name}_{layer}_emr_slave_sg",
                description=f"{self.name} {layer} EMR slave security group",
                init_ingress_rule="all_ssh",
            )
        for layer in redshift_layers:
            self.layer_sg_connectors["redshift"][layer] = AWSSecurityGroupConnector(
                vpc_id=vpc_config.vpc_id,
                group_name=f"{self.name}_{layer}_redshift_sg",
                description=f"{self.name} {layer} redshift security group",
            )
        self.state_handler_rds_connector = AWSRDSConnector(
            identifier=f"{self.name}-state-handler",
            sg_id=self.state_handler_sg_connector.update_or_create().group_id,
            subnet_group_name=subnet_group_config.name,
            **state_handler,
        )
        state_handler_rds_config: AWSRDSConfig = (
            self.state_handler_rds_connector.fetch_config()
        )
        super().__init__(
            name=name,
            log_folder=log_folder,
            bucket=bucket,
            s3_prefix=s3_prefix,
            layers={
                **{
                    layer: ManagedAWSLambdaLayer(
                        as_layer=layer,
                        bucket=self.bucket,
                        s3_prefix=self.get_layer_s3_prefix(layer),
                        queue_name=f"{self.name.lower()}-{layer.lower()}-sqs",
                        func_name=f"{self.name.lower()}-{layer.lower()}-lambda",
                        role_arn=self.iam_role_connectors[layer].update_or_create().arn,
                        code_config={
                            "S3Bucket": self.bucket,
                            "S3Key": self.s3_lambda_zip_key,
                        },
                        handler="aws_lambda_handlers.aws_lambda_apply_step_from_sqs",
                        s3_icp=self.s3_icp,
                        s3_fcp_template=self.s3_fcp_template,
                        environ={"RDS_PW": AWSRDSConfig.get_master_password()},
                        # security_group_id=self.layer_sg_connectors["lambda"][layer]
                        # .update_or_create()
                        # .group_id,
                        # subnet_ids=subnet_ids,
                        **layer_config,
                    )
                    for layer, layer_config in lambda_layers.items()
                },
                **{
                    layer: ManagedAWSEMRLayer(
                        as_layer=layer,
                        bucket=self.bucket,
                        s3_prefix=self.get_layer_s3_prefix(layer),
                        name=f"{self.name.lower()}-{layer.lower()}-emr",
                        log_uri=f"s3://{self.bucket}/{self.get_layer_s3_prefix(layer)}logs",
                        installer_uri=f"s3://{self.bucket}/{self.s3_bootstrap_emr}",
                        role_name=self.instance_profile_connectors[layer]
                        .update_or_create()
                        .name,
                        environ={
                            "RDS_PW": AWSRDSConfig.get_master_password(),
                            "REDSHIFT_PW": AWSRedshiftConfig.get_master_password(),
                        },
                        master_sg_id=self.layer_sg_connectors["emr"][f"{layer}_master"]
                        .update_or_create()
                        .group_id,
                        slave_sg_id=self.layer_sg_connectors["emr"][f"{layer}_slave"]
                        .update_or_create()
                        .group_id,
                        subnet_id=self.subnet_connectors[0]
                        .update_or_create()
                        .subnet_id,
                        **layer_config,
                    )
                    for layer, layer_config in emr_layers.items()
                },
                **{
                    layer: ManagedAWSRedshiftLayer(
                        as_layer=layer,
                        identifer=layer_config.get(
                            "identifier", f"{self.name.lower()}-redshift"
                        ),
                        role_arn=self.iam_role_connectors[layer].update_or_create().arn,
                        table_prefix=f"{layer}_",
                        sg_id=self.layer_sg_connectors["redshift"][layer]
                        .update_or_create()
                        .group_id,
                        cluster_subnet_group_name=cluster_subnet_group_config.name,
                        **{
                            key: val
                            for key, val in layer_config.items()
                            if key != "identifier"
                        },
                    )
                    for layer, layer_config in redshift_layers.items()
                },
            },
            state_handler_url=None
            if state_handler_rds_config is None
            else state_handler_rds_config.get_connection_string(),
        )

    def setup_implementer(self, icp: str):
        # Create bucket
        if self.bucket not in [
            bucket["Name"] for bucket in s3_client.list_buckets()["Buckets"]
        ]:
            logging.info("Creating bucket...")
            s3_client.create_bucket(
                Bucket=self.bucket,
                CreateBucketConfiguration={
                    "LocationConstraint": ADFGlobalConfig.AWS_REGION
                },
            )
        else:
            logging.info("Bucket already exists...")

        # Create log folder
        Path(self.log_folder).mkdir(parents=True, exist_ok=True)

        # Upload implementer config
        logging.info("Uploading implementer config...")
        s3_client.upload_file(Filename=icp, Bucket=self.bucket, Key=self.s3_icp)

        # Setup state handler
        logging.info("Setting up RDS DB for state handler...")
        self.state_handler = SQLStateHandler(
            self.state_handler_rds_connector.update_or_create().get_connection_string()
        )

        # Zip and upload code for EMR
        self.zip_emr()

        # Bootstrap for EMR
        yum_packages = ["python3-devel", "postgresql-devel", "emacs", "cmake"]
        bootstrap_script = BytesIO()
        bootstrap_script.write(
            (
                "\n".join(
                    [
                        "#!/bin/bash",
                        "set - euo pipefail",
                        "IFS=$'\n\t'",
                    ]
                    + [
                        f"sudo yum install -y {package} 1>> /home/hadoop/yum_install.out 2>> /home/hadoop/yum_install.err"
                        for package in yum_packages
                    ]
                    + [
                        f"mkdir /home/hadoop/adf 1>> /home/hadoop/sys.out 2>> /home/hadoop/sys.err",
                        f"cd /home/hadoop/adf/ 1>> /home/hadoop/sys.out 2>> /home/hadoop/sys.err",
                        f"aws s3 cp s3://{self.bucket}/{self.s3_emr_zip_key} /home/hadoop/adf/adf.zip 1>> /home/hadoop/sys.out 2>> /home/hadoop/sys.err",
                        f"unzip /home/hadoop/adf/adf.zip -d /home/hadoop/adf/ 1>> /home/hadoop/sys.out 2>> /home/hadoop/sys.err",
                        f"sudo pip3 install setuptools==38.5.1 1>> install.out 2>> install.err",  # setuptools on EMR seems broken...
                        f"sudo pip3 install Cython==0.29.24 1>> install.out 2>> install.err",
                        f"sudo pip3 install pyarrow==5.0.0 1>> install.out 2>> install.err",
                        f"sudo python3 setup.py install --emr 1>> /home/hadoop/setup.out 2>> /home/hadoop/setup.err",
                    ]
                    + [
                        f"cd /home/hadoop/adf/extra_packages/{extra_package_name} ; sudo python3 setup.py install 1>> /home/hadoop/setup.{extra_package_name}.out 2>> /home/hadoop/setup.{extra_package_name}.err"
                        for extra_package_name in [
                            os.path.basename(os.path.normpath(extra_package))
                            for extra_package in self.extra_packages
                        ]
                    ]
                )
                + "\n"
            ).encode("utf-8")
        )
        bootstrap_script.seek(0)
        s3_client.put_object(
            Bucket=self.bucket,
            Key=self.s3_bootstrap_emr,
            Body=bootstrap_script,
        )

        # Zip and upload code for lambda
        self.zip_lambda()

    def output_prebuilt_config(self, icp: str) -> Dict:
        return {
            self._class_key: "ADF.components.implementers.AWSImplementer",
            "mode": "prebuilt",
            "extra_packages": [
                os.path.relpath(extra_package) for extra_package in self.extra_packages
            ],
            "name": self.name,
            "log_folder": self.log_folder,
            "bucket": self.bucket,
            "s3_prefix": self.s3_prefix,
            "state_handler_url": self.state_handler.url,
            "lambda_layers": {
                layer_name: layer.output_prebuilt_config()
                for layer_name, layer in self.layers.items()
                if isinstance(layer, ManagedAWSLambdaLayer)
            },
            "emr_layers": {
                layer_name: layer.output_prebuilt_config()
                for layer_name, layer in self.layers.items()
                if isinstance(layer, ManagedAWSEMRLayer)
            },
            "redshift_layers": {
                layer_name: layer.output_prebuilt_config()
                for layer_name, layer in self.layers.items()
                if isinstance(layer, ManagedAWSRedshiftLayer)
            },
        }

    def destroy(self):
        try:
            run_command(f"rm {self.local_lambda_zip_path}")
        except RuntimeError:
            logging.info(f"Skipped deletion of {self.local_lambda_zip_path}")
        try:
            run_command(f"rm {self.local_emr_zip_path}")
        except RuntimeError:
            logging.info(f"Skipped deletion of {self.local_emr_zip_path}")
        s3_delete_prefix(self.bucket, self.s3_prefix)
        for profile in self.instance_profile_connectors.values():
            profile.destroy_if_exists()
        for iam_role in self.iam_role_connectors.values():
            iam_role.destroy_if_exists()
        self.state_handler_rds_connector.destroy_if_exists()
        self.subnet_group_connector.destroy_if_exists()
        self.cluster_subnet_group_connector.destroy_if_exists()
        for sg in [
            layer_sg
            for layer_type_sgs in self.layer_sg_connectors.values()
            for layer_sg in layer_type_sgs.values()
        ]:
            if sg.fetch_config() is not None:
                sg.reset_rules()
        for sg in [
            layer_sg
            for layer_type_sgs in self.layer_sg_connectors.values()
            for layer_sg in layer_type_sgs.values()
        ]:
            sg.destroy_if_exists()
        for subnet in self.subnet_connectors:
            subnet.destroy_if_exists()
        self.state_handler_sg_connector.destroy_if_exists()
        self.route_table_connector.destroy_if_exists()
        vpc_config = self.vpc_connector.fetch_config()
        internet_gateway_config = self.internet_gateway_connector.fetch_config()
        if vpc_config is not None and internet_gateway_config is not None:
            ec2_client.detach_internet_gateway(
                InternetGatewayId=internet_gateway_config.internet_gateway_id,
                VpcId=vpc_config.vpc_id,
            )
        self.internet_gateway_connector.destroy_if_exists()
        self.vpc_connector.destroy_if_exists()


class PrebuiltAWSImplementer(AWSImplementer):
    def __init__(
        self,
        name: str,
        log_folder: str,
        bucket: str,
        s3_prefix: str,
        state_handler_url: str,
        lambda_layers: Dict[str, Dict[str, str]],
        emr_layers: Dict[str, Dict[str, str]],
        redshift_layers: Dict[str, Dict[str, str]],
    ):
        self.s3_prefix = s3_prefix
        super().__init__(
            name=name,
            log_folder=log_folder,
            bucket=bucket,
            s3_prefix=s3_prefix,
            state_handler_url=state_handler_url,
            layers={
                **{
                    layer: PrebuiltAWSLambdaLayer(
                        as_layer=layer,
                        bucket=bucket,
                        s3_prefix=self.get_layer_s3_prefix(layer),
                        **layer_config,
                    )
                    for layer, layer_config in lambda_layers.items()
                },
                **{
                    layer: PrebuiltAWSEMRLayer(
                        as_layer=layer,
                        bucket=bucket,
                        s3_prefix=self.get_layer_s3_prefix(layer),
                        **layer_config,
                    )
                    for layer, layer_config in emr_layers.items()
                },
                **{
                    layer: PrebuiltAWSRedshiftLayer(as_layer=layer, **layer_config)
                    for layer, layer_config in redshift_layers.items()
                },
            },
        )

    def setup_implementer(self, icp: str) -> None:
        raise AWSUnmanagedOperation(self, "setup_implementer")

    def destroy(self) -> None:
        raise AWSUnmanagedOperation(self, "destroy")

    def output_prebuilt_config(self, icp: str) -> Dict:
        raise AWSUnmanagedOperation(self, "output_prebuilt_config")

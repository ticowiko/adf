import os
import logging
import subprocess

from abc import ABC, abstractmethod
from typing import Optional, Dict, List

from pyspark.sql import SparkSession

import ADF
from ADF.exceptions import AWSUnmanagedOperation
from ADF.components.data_structures import SparkDataStructure
from ADF.components.flow_config import ADFStep, ADFLandingStep, ADFCombinationStep
from ADF.components.layers import AbstractDataLayer
from ADF.utils import (
    AWSEMRConnector,
    AWSEMRConfig,
    AWSEMRServerlessConnector,
    AWSEMRServerlessConfig,
    emr_client,
    emr_serverless_client,
    s3_delete_prefix,
    s3_list_objects,
)
from ADF.config import ADFGlobalConfig


class AWSBaseEMRLayer(AbstractDataLayer, ABC):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        format: str = "csv",
        landing_format: Optional[str] = None,
    ):
        super().__init__(as_layer)
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        self.format = format
        self.landing_format = landing_format or format
        self._spark = None

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = SparkSession.builder.appName(
                f"emr-layer-{self.as_layer}"
            ).getOrCreate()
            self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return self._spark

    def setup_steps(self, steps: List[ADFStep]) -> None:
        pass

    def get_step_format(self, step: ADFStep) -> str:
        return self.landing_format if isinstance(step, ADFLandingStep) else self.format

    def get_step_prefix(self, step: ADFStep):
        return f"{self.s3_prefix}{step.flow.collection.name}/{step.flow.name}/{step.name}/{step.version}/"

    def get_landing_batch_key(self, step: ADFLandingStep, batch_id: str):
        return f"{self.get_step_prefix(step)}{batch_id}.{self.get_step_format(step)}"

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SparkDataStructure:
        # TODO : add schema in read if step meta is strict
        if isinstance(step, ADFLandingStep):
            return SparkDataStructure(
                df=self.spark.read.format(self.get_step_format(step))
                .options(header="true", inferSchema="true")
                .load(
                    f"s3a://{self.bucket}/{self.get_landing_batch_key(step, batch_id)}"
                )
            )
        else:
            df = (
                self.spark.read.format(self.get_step_format(step))
                .options(header="true", inferSchema="true")
                .load(f"s3a://{self.bucket}/{self.get_step_prefix(step)}")
            )
            return SparkDataStructure(
                df=df.filter(df[ADFGlobalConfig.BATCH_ID_COLUMN_NAME] == batch_id)
            )

    def read_full_data(self, step: ADFStep) -> SparkDataStructure:
        return SparkDataStructure(
            df=self.spark.read.format(self.get_step_format(step))
            .options(header="true", inferSchema="true")
            .load(f"s3a://{self.bucket}/{self.get_step_prefix(step)}")
        )

    def read_batches_data(
        self, step: ADFStep, batch_ids: List[str]
    ) -> Optional[SparkDataStructure]:
        if not batch_ids:
            return None
        if isinstance(step, ADFLandingStep):
            super().read_batches_data(step, batch_ids)
        else:
            ads = self.read_full_data(step)
            return ads[ads[ADFGlobalConfig.BATCH_ID_COLUMN_NAME].isin(batch_ids)]

    @staticmethod
    def get_step_partition_key(step: ADFStep) -> List[str]:
        return [ADFGlobalConfig.BATCH_ID_COLUMN_NAME] + sorted(step.get_partition_key())

    def write_batch_data(
        self, ads: SparkDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        all_cols = ads.list_columns()
        tech_cols = [
            ADFGlobalConfig.BATCH_ID_COLUMN_NAME,
            ADFGlobalConfig.TIMESTAMP_COLUMN_NAME,
        ]
        meta_cols = [col.name for col in step.meta.columns if col.name in all_cols]
        non_meta_cols = sorted(
            [col for col in all_cols if col not in (meta_cols + tech_cols)]
        )
        ads.df = ads.df.select(tech_cols + meta_cols + non_meta_cols)
        if self.get_step_format(step) == "csv":
            if isinstance(step, ADFLandingStep):
                ads.df.na.fill("").write.mode("overwrite").csv(
                    f"s3a://{self.bucket}/{self.get_step_prefix(step)}{batch_id}.csv",
                    header=True,
                )
            else:
                ads.df.na.fill("").write.mode("overwrite").partitionBy(
                    self.get_step_partition_key(step)
                ).csv(f"s3a://{self.bucket}/{self.get_step_prefix(step)}", header=True)
        elif self.get_step_format(step) == "parquet":
            if isinstance(step, ADFLandingStep):
                ads.df.write.mode("overwrite").parquet(
                    f"s3a://{self.bucket}/{self.get_step_prefix(step)}{batch_id}.parquet",
                )
            else:
                ads.df.write.mode("overwrite").partitionBy(
                    self.get_step_partition_key(step)
                ).parquet(f"s3a://{self.bucket}/{self.get_step_prefix(step)}")
        else:
            raise ValueError(
                f"Unknown format {self.get_step_format(step)} when writing to s3"
            )

    def key_to_batch_id(self, step: ADFStep, key: str) -> Optional[str]:
        step_prefix = self.get_step_prefix(step)
        if not key.startswith(step_prefix):
            return None
        batch_partition = key[len(step_prefix) :].split("/")[0]
        if "=" not in batch_partition:
            return None
        split = batch_partition.split("=")
        if (len(split) != 2) or (split[0] != ADFGlobalConfig.BATCH_ID_COLUMN_NAME):
            return None
        return split[1]

    def detect_batches(self, step: ADFStep) -> List[str]:
        if isinstance(step, ADFLandingStep):
            return [
                os.path.splitext(key.split("/")[-1])[0]
                for key in s3_list_objects(
                    bucket=self.bucket, prefix=self.get_step_prefix(step)
                )
                if key.endswith(f".{self.get_step_format(step)}")
            ]
        else:
            return [
                row[ADFGlobalConfig.BATCH_ID_COLUMN_NAME]
                for row in self.read_full_data(step)
                .distinct([ADFGlobalConfig.BATCH_ID_COLUMN_NAME])
                .to_list_of_dicts()
            ]

    def delete_step(self, step: ADFStep) -> None:
        s3_delete_prefix(
            self.bucket, self.get_step_prefix(step), ignore_nosuchbucket=True
        )

    def delete_batch(self, step: ADFStep, batch_id: str) -> None:
        s3_delete_prefix(
            self.bucket,
            f"{self.get_step_prefix(step)}{ADFGlobalConfig.BATCH_ID_COLUMN_NAME}={batch_id}/",
            ignore_nosuchbucket=True,
        )


class AWSEMRLayer(AWSBaseEMRLayer, ABC):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        format: str = "csv",
        landing_format: Optional[str] = None,
    ):
        super().__init__(
            as_layer=as_layer,
            bucket=bucket,
            s3_prefix=s3_prefix,
            format=format,
            landing_format=landing_format or format,
        )

    @property
    @abstractmethod
    def emr_config(self) -> AWSEMRConfig:
        pass

    @abstractmethod
    def setup_layer(self) -> None:
        pass

    @abstractmethod
    def destroy(self) -> None:
        pass

    def submit_step(
        self,
        step_in: ADFStep,
        step_out: ADFStep,
        batch_id: str,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ) -> Optional[subprocess.Popen]:
        if self.emr_config is None:
            raise ValueError(
                f"EMR config is null, are you sure the {str(self)} cluster is ready ?"
            )
        args = [
            "/usr/bin/spark-submit",
            "--conf",
            "spark.hadoop.fs.s3a.fast.upload=true",
            "--conf",
            "spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer",
            f"/home/hadoop/adf/bin/{implementer.get_exe_name()}",
            "/home/hadoop/implementer.yaml",
            "apply-step",
            f"/home/hadoop/flows.{step_out.flow.collection.name}.yaml",
            step_in.flow.name,
            step_in.name,
            step_out.flow.name,
            step_out.name,
            batch_id,
        ]
        logging.info(f"ADDING EMR STEP : {' '.join(args)}")
        emr_client.add_job_flow_steps(
            JobFlowId=self.emr_config.cluster_id,
            Steps=[
                {
                    "Name": f"{str(step_out)}-{batch_id}",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": args,
                    },
                }
            ],
        )
        return None

    def submit_combination_step(
        self,
        combination_step: ADFCombinationStep,
        batch_args: List[str],
        batch_id: str,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ) -> Optional[subprocess.Popen]:
        if self.emr_config is None:
            raise ValueError(
                f"EMR config is null, are you sure the {self.emr_config.name} cluster is ready ?"
            )
        args = [
            "/usr/bin/spark-submit",
            "--conf",
            "spark.hadoop.fs.s3a.fast.upload=true",
            "--conf",
            "spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer",
            f"/home/hadoop/adf/bin/{implementer.get_exe_name()}",
            "/home/hadoop/implementer.yaml",
            "apply-combination-step",
            f"/home/hadoop/flows.{combination_step.flow.collection.name}.yaml",
            combination_step.flow.name,
            combination_step.name,
            batch_id,
            *batch_args,
        ]
        logging.info(f"ADDING EMR STEP : {' '.join(args)}")
        emr_client.add_job_flow_steps(
            JobFlowId=self.emr_config.cluster_id,
            Steps=[
                {
                    "Name": f"{str(combination_step)}-{batch_id}",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": args,
                    },
                }
            ],
        )
        return None


class ManagedAWSEMRLayer(AWSEMRLayer):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        name: str,
        log_uri: str,
        installer_uri: str,
        role_name: str,
        format: str = "csv",
        landing_format: Optional[str] = None,
        environ: Optional[Dict] = None,
        master_instance_type: Optional[str] = None,
        slave_instance_type: Optional[str] = None,
        instance_count: Optional[int] = None,
        step_concurrency: Optional[int] = None,
        master_sg_id: Optional[str] = None,
        slave_sg_id: Optional[str] = None,
        subnet_id: Optional[str] = None,
    ):
        super().__init__(
            as_layer=as_layer,
            bucket=bucket,
            s3_prefix=s3_prefix,
            format=format,
            landing_format=landing_format,
        )
        self.emr_connector = AWSEMRConnector(
            name=name,
            log_uri=log_uri,
            installer_uri=installer_uri,
            role_name=role_name,
            environ=environ,
            master_instance_type=master_instance_type,
            slave_instance_type=slave_instance_type,
            instance_count=instance_count,
            step_concurrency=step_concurrency,
            master_sg_id=master_sg_id,
            slave_sg_id=slave_sg_id,
            subnet_id=subnet_id,
        )

    @property
    def emr_config(self) -> Optional[AWSEMRConfig]:
        return self.emr_connector.fetch_config()

    def setup_layer(self) -> None:
        self.emr_connector.update_or_create()

    def destroy(self) -> None:
        self.emr_connector.destroy_if_exists()
        s3_delete_prefix(self.bucket, self.s3_prefix, ignore_nosuchbucket=True)

    def output_prebuilt_config(self) -> Dict[str, str]:
        emr_config = self.emr_config
        return {
            "cluster_id": emr_config.cluster_id if emr_config else None,
            "cluster_arn": emr_config.cluster_arn if emr_config else None,
            "name": emr_config.name if emr_config else None,
            "public_dns": emr_config.public_dns if emr_config else None,
            "log_uri": emr_config.log_uri if emr_config else None,
            "format": self.format,
            "landing_format": self.landing_format,
        }


class PrebuiltAWSEMRLayer(AWSEMRLayer):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        cluster_id: str,
        cluster_arn: str,
        name: str,
        public_dns: str,
        log_uri: str,
        format: str = "csv",
        landing_format: Optional[str] = None,
    ):
        super().__init__(
            as_layer=as_layer,
            bucket=bucket,
            s3_prefix=s3_prefix,
            format=format,
            landing_format=landing_format,
        )
        self._emr_config = AWSEMRConfig(
            response={},
            cluster_id=cluster_id,
            cluster_arn=cluster_arn,
            name=name,
            public_dns=public_dns,
            log_uri=log_uri,
            step_concurrency=None,
        )

    @property
    def emr_config(self) -> AWSEMRConfig:
        return self._emr_config

    def setup_layer(self) -> None:
        raise AWSUnmanagedOperation(self, "setup_layer")

    def destroy(self) -> None:
        raise AWSUnmanagedOperation(self, "destroy")


class AWSEMRServerlessLayer(AWSBaseEMRLayer, ABC):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        role_arn: str,
        s3_launcher_key: str,
        venv_package_key: str,
        s3_icp: str,
        s3_fcp_template: str,
        format: str = "csv",
        landing_format: Optional[str] = None,
        environ: Optional[Dict] = None,
    ):
        super().__init__(
            as_layer=as_layer,
            bucket=bucket,
            s3_prefix=s3_prefix,
            format=format,
            landing_format=landing_format or format,
        )
        self.environ = environ or {}
        self.role_arn = role_arn
        self.s3_launcher_key = s3_launcher_key
        self.venv_package_key = venv_package_key
        self.s3_icp = s3_icp
        self.s3_fcp_template = s3_fcp_template

    @property
    @abstractmethod
    def emr_serverless_config(self) -> Optional[AWSEMRServerlessConfig]:
        pass

    @property
    def environ_conf_str(self) -> str:
        return " ".join(
            [
                f"--conf spark.emr-serverless.driverEnv.{key}={val}"
                for key, val in self.environ.items()
            ]
        )

    @property
    def conf_str(self) -> str:
        return " ".join(
            [
                f"--conf spark.archives=s3://{self.bucket}/{self.venv_package_key}#environment",
                "--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python",
                "--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python",
                "--conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
                self.environ_conf_str,
            ]
        )

    def get_log_path(self, step: ADFStep, batch_id: str) -> str:
        return f"s3://{self.bucket}/{self.s3_prefix}logs/{step.flow.name}/{step.name}/{step.version}/{batch_id}.logs"

    def submit_step(
        self,
        step_in: ADFStep,
        step_out: ADFStep,
        batch_id: str,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ) -> Optional[subprocess.Popen]:
        emr_serverless_config = self.emr_serverless_config
        if emr_serverless_config is None:
            raise ValueError(
                f"EMR Serverless config is null, are you sure the {str(self)} application is ready ?"
            )
        emr_serverless_client.start_job_run(
            name=f"{step_out.flow.name}:{step_out.name}:{step_out.version}::{batch_id}",
            applicationId=emr_serverless_config.application_id,
            executionRoleArn=self.role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"s3://{self.bucket}/{self.s3_launcher_key}",
                    "entryPointArguments": [
                        self.s3_icp,
                        "apply-step",
                        self.s3_fcp_template.format(
                            collection_name=step_out.flow.collection.name
                        ),
                        step_in.flow.name,
                        step_in.name,
                        step_out.flow.name,
                        step_out.name,
                        batch_id,
                    ],
                    "sparkSubmitParameters": self.conf_str,
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": self.get_log_path(step_out, batch_id)
                    }
                }
            },
        )
        return None

    def submit_combination_step(
        self,
        combination_step: ADFCombinationStep,
        batch_args: List[str],
        batch_id: str,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ) -> Optional[subprocess.Popen]:
        emr_serverless_config = self.emr_serverless_config
        if emr_serverless_config is None:
            raise ValueError(
                f"EMR Serverless config is null, are you sure the {str(self)} application is ready ?"
            )
        emr_serverless_client.start_job_run(
            name=f"{combination_step.flow.name}:{combination_step.name}:{combination_step.version}::{batch_id}",
            applicationId=emr_serverless_config.application_id,
            executionRoleArn=self.role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"s3://{self.bucket}/{self.s3_launcher_key}",
                    "entryPointArguments": [
                        self.s3_icp,
                        "apply-combination-step",
                        self.s3_fcp_template.format(
                            collection_name=combination_step.flow.collection.name
                        ),
                        combination_step.flow.name,
                        combination_step.name,
                        batch_id,
                        *batch_args,
                    ],
                    "sparkSubmitParameters": self.conf_str,
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": self.get_log_path(combination_step, batch_id)
                    }
                }
            },
        )
        return None


class ManagedAWSEMRServerlessLayer(AWSEMRServerlessLayer):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        name: str,
        role_arn: str,
        s3_launcher_key: str,
        venv_package_key: str,
        s3_icp: str,
        s3_fcp_template: str,
        format: str = "csv",
        landing_format: Optional[str] = None,
        environ: Optional[Dict] = None,
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
        super().__init__(
            as_layer=as_layer,
            bucket=bucket,
            s3_prefix=s3_prefix,
            role_arn=role_arn,
            s3_launcher_key=s3_launcher_key,
            venv_package_key=venv_package_key,
            s3_icp=s3_icp,
            s3_fcp_template=s3_fcp_template,
            format=format,
            landing_format=landing_format or format,
            environ=environ,
        )
        self.emr_serverless_connector = AWSEMRServerlessConnector(
            name=name,
            release_label=release_label,
            initial_driver_worker_count=initial_driver_worker_count,
            initial_driver_cpu=initial_driver_cpu,
            initial_driver_memory=initial_driver_memory,
            initial_executor_worker_count=initial_executor_worker_count,
            initial_executor_cpu=initial_executor_cpu,
            initial_executor_memory=initial_executor_memory,
            max_cpu=max_cpu,
            max_memory=max_memory,
            idle_timeout_minutes=idle_timeout_minutes,
            sg_id=sg_id,
            subnet_id=subnet_id,
        )

    @property
    def emr_serverless_config(self) -> Optional[AWSEMRServerlessConfig]:
        return self.emr_serverless_connector.fetch_config()

    def setup_layer(self) -> None:
        self.emr_serverless_connector.update_or_create()

    def destroy(self) -> None:
        self.emr_serverless_connector.destroy_if_exists()
        s3_delete_prefix(self.bucket, self.s3_prefix, ignore_nosuchbucket=True)

    def output_prebuilt_config(self) -> Dict[str, str]:
        emr_serverless_config = self.emr_serverless_config
        return {
            "application_id": emr_serverless_config.application_id,
            "bucket": self.bucket,
            "s3_prefix": self.s3_prefix,
            "role_arn": self.role_arn,
            "s3_launcher_key": self.s3_launcher_key,
            "venv_package_key": self.venv_package_key,
            "s3_icp": self.s3_icp,
            "s3_fcp_template": self.s3_fcp_template,
            "format": self.format,
            "landing_format": self.landing_format,
            "environ": self.environ,
        }


class PrebuiltAWSEMRServerlessLayer(AWSEMRServerlessLayer):
    def __init__(
        self,
        as_layer: str,
        application_id: str,
        bucket: str,
        s3_prefix: str,
        role_arn: str,
        s3_launcher_key: str,
        venv_package_key: str,
        s3_icp: str,
        s3_fcp_template: str,
        format: str = "csv",
        landing_format: Optional[str] = None,
        environ: Optional[Dict] = None,
    ):
        super().__init__(
            as_layer=as_layer,
            bucket=bucket,
            s3_prefix=s3_prefix,
            role_arn=role_arn,
            s3_launcher_key=s3_launcher_key,
            venv_package_key=venv_package_key,
            s3_icp=s3_icp,
            s3_fcp_template=s3_fcp_template,
            format=format,
            landing_format=landing_format or format,
            environ=environ,
        )
        self._emr_serverless_config = AWSEMRServerlessConfig(
            response={},
            application_id=application_id,
            name="",
            arn="",
            state="",
            application_type="",
            release_label="",
            initial_driver_worker_count=-1,
            initial_driver_cpu="",
            initial_driver_memory="",
            initial_driver_disk="",
            initial_executor_worker_count=-1,
            initial_executor_cpu="",
            initial_executor_memory="",
            initial_executor_disk="",
            max_cpu="",
            max_memory="",
            idle_timeout_minutes=-1,
            sg_id="",
            subnet_id="",
        )

    @property
    def emr_serverless_config(self) -> AWSEMRServerlessConfig:
        return self._emr_serverless_config

    def setup_layer(self) -> None:
        raise AWSUnmanagedOperation(self, "setup_layer")

    def destroy(self) -> None:
        raise AWSUnmanagedOperation(self, "destroy")

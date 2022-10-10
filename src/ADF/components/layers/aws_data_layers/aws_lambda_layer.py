import os
import json
import logging
import subprocess

from abc import ABC, abstractmethod
from io import StringIO, BytesIO
from typing import List, Dict, Optional

import ADF
from ADF.exceptions import AWSUnmanagedOperation
from ADF.components.data_structures import PandasDataStructure
from ADF.components.flow_config import ADFStep, ADFCombinationStep
from ADF.components.layers import AbstractDataLayer
from ADF.utils import (
    MetaCSVToPandas,
    PandasToMetaCSV,
    s3_client,
    sqs_client,
    s3_delete_prefix,
    s3_list_objects,
    AWSSQSConfig,
    AWSSQSConnector,
    AWSLambdaConnector,
    AWSLambdaConfig,
    AWSEventSourceMappingConnector,
)


class AWSLambdaLayer(AbstractDataLayer, ABC):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        s3_icp: str,
        s3_fcp_template: str,
        sep: str = ",",
    ):
        super().__init__(as_layer)
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        self.s3_icp = s3_icp
        self.s3_fcp_template = s3_fcp_template
        self.sep = sep

    @property
    @abstractmethod
    def sqs_config(self) -> Optional[AWSSQSConfig]:
        pass

    @property
    @abstractmethod
    def lambda_config(self) -> Optional[AWSLambdaConfig]:
        pass

    @abstractmethod
    def setup_layer(self) -> None:
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
        sqs_config = self.sqs_config
        sqs_client.send_message(
            QueueUrl=sqs_config.url,
            MessageBody=json.dumps(
                {
                    "is_combination_step": False,
                    "as_layer": self.as_layer,
                    "bucket": self.bucket,
                    "s3_icp": self.s3_icp,
                    "s3_fcp": self.s3_fcp_template.format(
                        collection_name=step_out.flow.collection.name
                    ),
                    "batch_id": batch_id,
                    "step_in_flow_name": step_in.flow.name,
                    "step_in_step_name": step_in.name,
                    "step_out_flow_name": step_out.flow.name,
                    "step_out_step_name": step_out.name,
                }
            ),
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
        sqs_config = self.sqs_config
        sqs_client.send_message(
            QueueUrl=sqs_config.url,
            MessageBody=json.dumps(
                {
                    "is_combination_step": True,
                    "as_layer": self.as_layer,
                    "bucket": self.bucket,
                    "s3_icp": self.s3_icp,
                    "s3_fcp": self.s3_fcp_template.format(
                        collection_name=combination_step.flow.collection.name
                    ),
                    "batch_args": batch_args,
                    "batch_id": batch_id,
                    "combination_step_flow_name": combination_step.flow.name,
                    "combination_step_step_name": combination_step.name,
                }
            ),
        )
        return None

    def key_to_batch(self, step: ADFStep, key: str) -> str:
        step_prefix = self.step_prefix(step)
        if not key.startswith(step_prefix):
            raise ValueError(
                f"Given key {key} does not start with step prefix {self.step_prefix(step)}"
            )
        return key[len(step_prefix) : -4]

    def batch_to_key(self, step: ADFStep, batch_id: str) -> str:
        return f"{self.step_prefix(step)}{batch_id}.csv"

    def step_prefix(self, step: ADFStep) -> str:
        return f"{self.s3_prefix}{step.flow.collection.name}/{step.flow.name}/{step.name}/{step.version}/"

    def setup_steps(self, steps: List[ADFStep]) -> None:
        pass

    def read_batch_data(self, step: ADFStep, batch_id: str) -> PandasDataStructure:
        logging.info(f"Reading from step {str(step)} for batch {batch_id}...")
        return PandasDataStructure(
            MetaCSVToPandas(
                StringIO(
                    s3_client.get_object(
                        Bucket=self.bucket, Key=self.batch_to_key(step, batch_id)
                    )["Body"]
                    .read()
                    .decode("utf-8")
                ),
                sep=self.sep,
            ).get_df()
        )

    def read_full_data(self, step: ADFStep) -> PandasDataStructure:
        pds_list = [
            self.read_batch_data(step, self.key_to_batch(step, key))
            for key in s3_list_objects(
                bucket=self.bucket, prefix=self.step_prefix(step)
            )
            if key.endswith(".csv")
        ]
        return pds_list[0].union(*pds_list[1:])

    def write_batch_data(
        self, ads: PandasDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        logging.info(f"Writing to step {str(step)} for batch {batch_id}...")
        output = BytesIO()
        PandasToMetaCSV(ads.df, sep=self.sep).write_file(output)
        output.seek(0)
        s3_client.put_object(
            Bucket=self.bucket,
            Key=self.batch_to_key(step, batch_id),
            Body=output,
        )

    def detect_batches(self, step: ADFStep) -> List[str]:
        return [
            os.path.splitext(key.split("/")[-1])[0]
            for key in s3_list_objects(
                bucket=self.bucket, prefix=self.step_prefix(step)
            )
            if key.endswith(".csv")
        ]

    def delete_step(self, step: ADFStep) -> None:
        s3_delete_prefix(self.bucket, self.step_prefix(step), ignore_nosuchbucket=True)

    def delete_batch(self, step: ADFStep, batch_id: str) -> None:
        s3_delete_prefix(
            self.bucket, self.batch_to_key(step, batch_id), ignore_nosuchbucket=True
        )

    @abstractmethod
    def destroy(self) -> None:
        pass


class ManagedAWSLambdaLayer(AWSLambdaLayer):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        queue_name: str,
        func_name: str,
        role_arn: str,
        code_config: Dict,
        handler: str,
        s3_icp: str,
        s3_fcp_template: str,
        environ: Optional[Dict[str, str]] = None,
        sep: str = ",",
        timeout: int = 60,
        memory: int = 128,
        security_group_id: Optional[str] = None,
        subnet_ids: Optional[List[str]] = None,
    ):
        super().__init__(
            as_layer=as_layer,
            bucket=bucket,
            s3_prefix=s3_prefix,
            s3_icp=s3_icp,
            s3_fcp_template=s3_fcp_template,
            sep=sep,
        )
        self.sqs_connector = AWSSQSConnector(name=queue_name)
        self.lambda_connector = AWSLambdaConnector(
            func_name=func_name,
            role_arn=role_arn,
            code_config=code_config,
            handler=handler,
            environ=environ,
            timeout=timeout,
            memory=memory,
            security_group_id=security_group_id,
            subnet_ids=subnet_ids,
        )

    @property
    def sqs_config(self) -> Optional[AWSSQSConfig]:
        return self.sqs_connector.fetch_config()

    @property
    def lambda_config(self) -> Optional[AWSLambdaConfig]:
        return self.lambda_connector.fetch_config()

    def construct_event_mapping_connector(
        self,
        sqs_config: Optional[AWSSQSConfig] = None,
        lambda_config: Optional[AWSLambdaConfig] = None,
    ) -> Optional[AWSEventSourceMappingConnector]:
        sqs_config = sqs_config or self.sqs_config
        lambda_config = lambda_config or self.lambda_config
        if sqs_config is None or lambda_config is None:
            return None
        return AWSEventSourceMappingConnector(
            source_arn=sqs_config.arn,
            function_name=lambda_config.name,
        )

    def destroy(self) -> None:
        event_mapping_connector = self.construct_event_mapping_connector()
        if event_mapping_connector is not None:
            event_mapping_connector.destroy_if_exists()
        self.lambda_connector.destroy_if_exists()
        self.sqs_connector.destroy_if_exists()
        s3_delete_prefix(self.bucket, self.s3_prefix, ignore_nosuchbucket=True)

    def setup_layer(self) -> None:
        sqs_config: AWSSQSConfig = self.sqs_connector.update_or_create()
        lambda_config: AWSLambdaConfig = self.lambda_connector.update_or_create()
        self.construct_event_mapping_connector(
            sqs_config=sqs_config, lambda_config=lambda_config
        ).update_or_create()

    def output_prebuilt_config(self) -> Dict[str, str]:
        sqs_config = self.sqs_config
        lambda_config = self.lambda_config
        return {
            "bucket": self.bucket,
            "s3_prefix": self.s3_prefix,
            "s3_icp": self.s3_icp,
            "s3_fcp_template": self.s3_fcp_template,
            "sqs_name": sqs_config.name if sqs_config else None,
            "sqs_arn": sqs_config.arn if sqs_config else None,
            "sqs_url": sqs_config.url if sqs_config else None,
            "lambda_name": lambda_config.name if lambda_config else None,
            "lambda_arn": lambda_config.arn if lambda_config else None,
            "sep": self.sep,
        }


class PrebuiltAWSLambdaLayer(AWSLambdaLayer):
    def __init__(
        self,
        as_layer: str,
        bucket: str,
        s3_prefix: str,
        s3_icp: str,
        s3_fcp_template: str,
        sqs_name: str,
        sqs_arn: str,
        sqs_url: str,
        lambda_name: str,
        lambda_arn: str,
        sep: str = ",",
    ):
        super().__init__(
            as_layer=as_layer,
            bucket=bucket,
            s3_prefix=s3_prefix,
            s3_icp=s3_icp,
            s3_fcp_template=s3_fcp_template,
            sep=sep,
        )
        self._sqs_config = AWSSQSConfig(
            response={},
            name=sqs_name,
            arn=sqs_arn,
            url=sqs_url,
        )
        self._lambda_config = AWSLambdaConfig(
            response={},
            name=lambda_name,
            arn=lambda_arn,
        )

    @property
    def sqs_config(self) -> AWSSQSConfig:
        return self._sqs_config

    @property
    def lambda_config(self) -> AWSLambdaConfig:
        return self._lambda_config

    def setup_layer(self) -> None:
        raise AWSUnmanagedOperation(self, "setup_layer")

    def destroy(self) -> None:
        raise AWSUnmanagedOperation(self, "destroy")

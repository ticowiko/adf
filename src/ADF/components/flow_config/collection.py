import logging

import yaml

from typing import List, TextIO

from ADF.exceptions import UnknownFlow, InvalidAbstractDataFlow, InvalidADFCollection
from ADF.components.flow_config import (
    ADFStep,
    ADFReceptionStep,
    ADFCombinationStep,
    AbstractDataFlow,
    ADFModule,
)
from ADF.config import ADFGlobalConfig
from ADF.utils import (
    ToDictMixin,
    extract_parameter,
    s3_client,
    s3_url_to_bucket_and_key,
)


class ADFCollection(ToDictMixin):
    flat_attrs = ["name"]
    iter_attrs = ["flows"]

    def __init__(self, name: str, flows: List[AbstractDataFlow]):
        self.name = name
        for flow in flows:
            flow.collection = self
        self.flows = flows
        self.validate()

    @classmethod
    def from_config_path(cls, config_path: str) -> "ADFCollection":
        if config_path.startswith("s3://"):
            logging.info(f"Loading {cls.__name__} from path {config_path}")
            bucket, key = s3_url_to_bucket_and_key(config_path)
            return cls.from_yaml_string(
                s3_client.get_object(
                    Bucket=bucket,
                    Key=key,
                )["Body"]
                .read()
                .decode("utf-8")
            )
        else:
            return cls.from_config_file(open(config_path, "r"))

    @classmethod
    def from_config_file(cls, config_file: TextIO) -> "ADFCollection":
        return cls.from_yaml_file(config_file)

    @classmethod
    def from_yaml_file(cls, yaml_file: TextIO) -> "ADFCollection":
        return cls.from_config(yaml.load(yaml_file.read(), Loader=yaml.Loader))

    @classmethod
    def from_yaml_string(cls, yaml_string: str) -> "ADFCollection":
        return cls.from_config(yaml.safe_load(yaml_string))

    @classmethod
    def from_config(cls, config: dict):
        name = extract_parameter(config, "name", "flow collection config")
        default_step_version = config.get("DEFAULT_STEP_VERSION", "default")
        ADFGlobalConfig.BATCH_ID_COLUMN_NAME = config.get(
            "BATCH_ID_COLUMN_NAME", ADFGlobalConfig.BATCH_ID_COLUMN_NAME
        )
        ADFGlobalConfig.SQL_PK_COLUMN_NAME = config.get(
            "SQL_PK_COLUMN_NAME", ADFGlobalConfig.SQL_PK_COLUMN_NAME
        )
        ADFGlobalConfig.TIMESTAMP_COLUMN_NAME = config.get(
            "TIMESTAMP_COLUMN_NAME", ADFGlobalConfig.TIMESTAMP_COLUMN_NAME
        )
        ADFModule.from_config(config.get("modules", []))
        flows = []
        for flow_config in extract_parameter(config, "flows", "flow collection config"):
            flow = AbstractDataFlow(
                extract_parameter(flow_config, "name", "unknown flow")
            )
            for step_config in extract_parameter(
                flow_config, "steps", f"flow {flow_config['name']}"
            ):
                step_config["version"] = step_config.get(
                    "version", default_step_version
                )
                ADFStep.from_config(
                    step_config=step_config,
                    flow=flow,
                    flows=flows,
                    attach=True,
                )
            flows.append(flow)
        return cls(name=name, flows=flows)

    def __str__(self):
        return f"{self.__class__.__name__}::{self.name}"

    def __iter__(self):
        return self.flows.__iter__()

    def __next__(self):
        return self.flows.__next__()

    @staticmethod
    def get_flow_from_flows(
        flows: List[AbstractDataFlow], flow_name: str
    ) -> AbstractDataFlow:
        for flow in flows:
            if flow.name == flow_name:
                return flow
        raise UnknownFlow(flow_name)

    def get_flow(self, flow_name: str) -> AbstractDataFlow:
        return self.get_flow_from_flows(self.flows, flow_name)

    def get_step(self, flow_name: str, step_name: str):
        return self.get_flow(flow_name).get_step(step_name)

    def validate(self) -> None:
        errs = []
        seen = set()
        for flow in self.flows:
            try:
                flow.validate()
            except InvalidAbstractDataFlow as e:
                errs += e.errs
            if flow.name in seen:
                errs.append(f"Flow '{flow.name}' duplicated in ADF config {str(self)}.")
            seen.add(flow.name)
        if "/" in self.name:
            errs.append(f"Collection name '{self.name}' contains '/'.")
        if errs:
            raise InvalidADFCollection(errs)

    def get_output_steps(self, step: ADFStep) -> List[ADFReceptionStep]:
        output_steps: List[ADFReceptionStep] = []
        for flow in self.flows:
            initial_step = flow.steps[0]
            if isinstance(initial_step, ADFReceptionStep):
                if step in initial_step.input_steps:
                    output_steps.append(initial_step)
        return output_steps

    def get_downstream_combination_steps(
        self, step: ADFStep
    ) -> List[ADFCombinationStep]:
        output_steps: List[ADFCombinationStep] = []
        for flow in self.flows:
            initial_step = flow.steps[0]
            if isinstance(initial_step, ADFCombinationStep):
                if step in initial_step.input_steps:
                    output_steps.append(initial_step)
        return output_steps

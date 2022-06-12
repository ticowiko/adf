import datetime
import croniter

from abc import ABC, abstractmethod
from typing import Dict, List

import ADF
from ADF.utils import extract_parameter
from ADF.config import ADFGlobalConfig


class ADFBatchDependencyHandler(ABC):
    @classmethod
    @abstractmethod
    def from_config(cls, config: Dict) -> "ADFBatchDependencyHandler":
        pass

    @abstractmethod
    def get_dependent_batches(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
    ) -> List[str]:
        pass

    def dependent_batches(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
    ) -> List[str]:
        return [
            str(batch_id)
            for batch_id in self.get_dependent_batches(
                state_handler=state_handler,
                step_in=step_in,
                step_out=step_out,
                batch_id=batch_id,
            )
            if (str(batch_id) in state_handler.get_step_all(step_out))
        ]


class DefaultBatchDependencyHandler(ADFBatchDependencyHandler):
    @classmethod
    def from_config(cls, config: Dict) -> "ADFBatchDependencyHandler":
        return cls()

    def get_dependent_batches(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
    ) -> List[str]:
        return [batch_id]


class NullBatchDependencyHandler(ADFBatchDependencyHandler):
    @classmethod
    def from_config(cls, config: Dict) -> "NullBatchDependencyHandler":
        return cls()

    def get_dependent_batches(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
    ) -> List[str]:
        return []


class TimeDeltaBatchDependencyHandler(ADFBatchDependencyHandler):
    delta_unit_dict: Dict[str, int] = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 60 * 60 * 24,
        "w": 60 * 60 * 24 * 7,
    }

    def __init__(self, delta: int, only_success: bool = False, delta_unit: str = "s"):
        self.delta_seconds = delta * self.delta_unit_dict[delta_unit]
        self.only_success = only_success

    @classmethod
    def from_config(cls, config: Dict) -> "TimeDeltaBatchDependencyHandler":
        return cls(
            delta=extract_parameter(config, "delta_seconds", f"Class {cls.__name__}"),
            only_success=config.get("only_success", False),
            delta_unit=config.get("delta_unit", "s"),
        )

    def get_dependent_batches(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
    ) -> List[str]:
        dependent_batches = []
        batch_info = state_handler.get_batch_info(step_in, batch_id)
        if self.only_success and batch_info["status"] != ADFGlobalConfig.STATUS_SUCCESS:
            return []
        batch_time: datetime.datetime = batch_info["datetime"]
        for downstream_batch_id in state_handler.get_step_all(step_out):
            max_time = datetime.datetime.strptime(
                downstream_batch_id, ADFGlobalConfig.BATCH_TIME_FORMAT_STRING
            )
            min_time = max_time - datetime.timedelta(seconds=self.delta_seconds)
            if min_time < batch_time < max_time:
                dependent_batches.append(downstream_batch_id)
        return dependent_batches


class CronBatchDependencyHandler(ADFBatchDependencyHandler):
    def __init__(
        self,
        cron_expression: str,
        only_success: bool = False,
        strict: bool = True,
    ):
        self.cron_expression = cron_expression
        self.only_success = only_success
        self.strict = strict

    @classmethod
    def from_config(cls, config: Dict) -> "ADFBatchDependencyHandler":
        return cls(
            cron_expression=extract_parameter(
                config, "cron_expression", f"Class {cls.__name__}"
            ),
            only_success=config.get("only_success", False),
            strict=config.get("strict", True),
        )

    def get_dependent_batches(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
    ) -> List[str]:
        dependent_batches = []
        batch_info = state_handler.get_batch_info(step_in, batch_id)
        if self.only_success and batch_info["status"] != ADFGlobalConfig.STATUS_SUCCESS:
            return []
        batch_time: datetime.datetime = batch_info["datetime"]
        for downstream_batch_id in state_handler.get_step_all(step_out):
            max_time = datetime.datetime.strptime(
                downstream_batch_id, ADFGlobalConfig.BATCH_TIME_FORMAT_STRING
            )
            min_time = croniter.croniter(self.cron_expression, max_time).get_prev(
                datetime.datetime
            )
            if self.strict and (
                croniter.croniter(self.cron_expression, min_time).get_next(
                    datetime.datetime
                )
                != max_time
            ):
                raise ValueError(
                    f"{self.__class__.__name__} failed strict cron checking"
                )
            if min_time < batch_time < max_time:
                dependent_batches.append(downstream_batch_id)
        return dependent_batches

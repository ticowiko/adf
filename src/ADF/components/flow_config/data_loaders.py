import datetime
import croniter

from abc import ABC, abstractmethod
from typing import Dict, Tuple, List, Optional

import ADF
from ADF.utils import extract_parameter
from ADF.config import ADFGlobalConfig


class ADFDataLoader(ABC):
    @classmethod
    @abstractmethod
    def from_config(cls, config: Dict) -> "ADFDataLoader":
        pass

    @abstractmethod
    def get_ads_args(
        self,
        data_interface: "ADF.components.layers.AbstractDataInterface",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    ) -> Tuple[
        List["ADF.components.data_structures.AbstractDataStructure"],
        Dict[str, "ADF.components.data_structures.AbstractDataStructure"],
    ]:
        pass


class DefaultDataLoader(ADFDataLoader):
    @classmethod
    def from_config(cls, config: Dict) -> "DefaultDataLoader":
        return cls()

    def get_ads_args(
        self,
        data_interface: "ADF.components.layers.AbstractDataInterface",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    ) -> Tuple[
        List["ADF.components.data_structures.AbstractDataStructure"],
        Dict[str, "ADF.components.data_structures.AbstractDataStructure"],
    ]:
        return [data_interface.read_batch_data(step_in, batch_id)], {}


class NullDataLoader(ADFDataLoader):
    @classmethod
    def from_config(cls, config: Dict) -> "NullDataLoader":
        return cls()

    def get_ads_args(
        self,
        data_interface: "ADF.components.layers.AbstractDataInterface",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    ) -> Tuple[
        List["ADF.components.data_structures.AbstractDataStructure"],
        Dict[str, "ADF.components.data_structures.AbstractDataStructure"],
    ]:
        return [], {}


class KwargDataLoader(ADFDataLoader):
    def __init__(self, kwarg_name: str):
        self.kwarg_name = kwarg_name

    @classmethod
    def from_config(cls, config: Dict) -> "KwargDataLoader":
        return cls(
            kwarg_name=extract_parameter(
                config, "kwarg_name", f"Loading {cls.__name__}"
            )
        )

    def get_ads_args(
        self,
        data_interface: "ADF.components.layers.AbstractDataInterface",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    ) -> Tuple[
        List["ADF.components.data_structures.AbstractDataStructure"],
        Dict[str, "ADF.components.data_structures.AbstractDataStructure"],
    ]:
        return [], {self.kwarg_name: data_interface.read_batch_data(step_in, batch_id)}


class FullDataLoader(ADFDataLoader):
    def __init__(
        self,
        kwarg: Optional[str] = None,
    ):
        super().__init__()
        self.kwarg = kwarg

    @classmethod
    def from_config(cls, config: Dict) -> "FullDataLoader":
        return cls(
            kwarg=config.get("kwarg", None),
        )

    def get_ads_args(
        self,
        data_interface: "ADF.components.layers.AbstractDataInterface",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    ) -> Tuple[
        List["ADF.components.data_structures.AbstractDataStructure"],
        Dict[str, "ADF.components.data_structures.AbstractDataStructure"],
    ]:
        ads = data_interface.read_full_data(step_in)
        return ([], {self.kwarg: ads}) if self.kwarg is not None else ([ads], {})


class FullAndIncomingDataLoader(ADFDataLoader):
    @classmethod
    def from_config(cls, config: Dict) -> "FullAndIncomingDataLoader":
        return cls()

    def get_ads_args(
        self,
        data_interface: "ADF.components.layers.AbstractDataInterface",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    ) -> Tuple[
        List["ADF.components.data_structures.AbstractDataStructure"],
        Dict[str, "ADF.components.data_structures.AbstractDataStructure"],
    ]:
        return [], {
            "incoming_ads": data_interface.read_batch_data(step_in, batch_id),
            "full_ads": data_interface.read_full_data(step_in),
        }


class TimeDeltaDataLoader(ADFDataLoader):
    delta_unit_dict: Dict[str, int] = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 60 * 60 * 24,
        "w": 60 * 60 * 24 * 7,
    }

    def __init__(
        self,
        delta: int,
        delta_unit: str = "s",
        kwarg: Optional[str] = None,
    ):
        self.delta_seconds = delta * self.delta_unit_dict[delta_unit]
        self.kwarg = kwarg

    @classmethod
    def from_config(cls, config: Dict) -> "TimeDeltaDataLoader":
        return cls(
            delta=extract_parameter(config, "delta", f"Class {cls.__name__}"),
            delta_unit=config.get("delta_unit", "s"),
            kwarg=config.get("kwarg", None),
        )

    def get_ads_args(
        self,
        data_interface: "ADF.components.layers.AbstractDataInterface",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    ) -> Tuple[
        List["ADF.components.data_structures.AbstractDataStructure"],
        Dict[str, "ADF.components.data_structures.AbstractDataStructure"],
    ]:
        max_time = datetime.datetime.strptime(
            batch_id, ADFGlobalConfig.BATCH_TIME_FORMAT_STRING
        )
        min_time = max_time - datetime.timedelta(seconds=self.delta_seconds)
        state_ads = state_handler.to_step_ads(step_in)
        batch_ids = [
            entry["batch_id"]
            for entry in state_ads[
                (state_ads["datetime"] > min_time)
                & (state_ads["datetime"] < max_time)
                & (state_ads["status"] == ADFGlobalConfig.STATUS_SUCCESS)
            ].to_list_of_dicts()
        ]
        ads = data_interface.read_batches_data(step_in, batch_ids)
        if ads is None:
            raise ValueError(
                f"Found no data in requested time range {min_time} -> {max_time} for step {str(step_in)}"
            )
        return ([], {self.kwarg: ads}) if self.kwarg else ([ads], {})


class CronDataLoader(ADFDataLoader):
    def __init__(
        self,
        cron_expression: str,
        strict: bool = True,
        kwarg: Optional[str] = None,
    ):
        super().__init__()
        self.cron_expression = cron_expression
        self.strict = strict
        self.kwarg = kwarg

    @classmethod
    def from_config(cls, config: Dict) -> "CronDataLoader":
        return cls(
            cron_expression=extract_parameter(
                config, "cron_expression", f"Class {cls.__name__}"
            ),
            strict=config.get("strict", True),
            kwarg=config.get("kwarg", None),
        )

    def get_ads_args(
        self,
        data_interface: "ADF.components.layers.AbstractDataInterface",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
        batch_id: str,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    ) -> Tuple[
        List["ADF.components.data_structures.AbstractDataStructure"],
        Dict[str, "ADF.components.data_structures.AbstractDataStructure"],
    ]:
        max_time = datetime.datetime.strptime(
            batch_id, ADFGlobalConfig.BATCH_TIME_FORMAT_STRING
        )
        min_time: datetime.datetime = croniter.croniter(
            self.cron_expression, max_time
        ).get_prev(datetime.datetime)
        if self.strict and (
            croniter.croniter(self.cron_expression, min_time).get_next(
                datetime.datetime
            )
            != max_time
        ):
            raise ValueError(f"{self.__class__.__name__} failed strict cron checking")
        state_ads = state_handler.to_step_ads(step_in)
        batch_ids = [
            entry["batch_id"]
            for entry in state_ads[
                (state_ads["datetime"] > min_time)
                & (state_ads["datetime"] < max_time)
                & (state_ads["status"] == ADFGlobalConfig.STATUS_SUCCESS)
            ].to_list_of_dicts()
        ]
        ads = data_interface.read_batches_data(step_in, batch_ids)
        if ads is None:
            raise ValueError(
                f"Found no data in requested time range {min_time} -> {max_time} for step {str(step_in)}"
            )
        return ([], {self.kwarg: ads}) if self.kwarg else ([ads], {})

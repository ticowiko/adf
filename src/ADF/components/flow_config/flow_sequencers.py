import datetime
import croniter

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Tuple, Type

import ADF
from ADF.components.flow_config import ADFFunction
from ADF.config import ADFGlobalConfig
from ADF.exceptions import CombinationStepError
from ADF.utils import extract_parameter


def get_ignore_list(
    state_handler: "ADF.components.state_handlers.AbstractStateHandler",
    step: "ADF.components.flow_config.ADFStep",
    redo: bool,
) -> List[str]:
    ads = state_handler.to_step_ads(step)
    return (
        [
            entry["batch_id"]
            for entry in ads[
                ads["status"].isin(
                    [
                        ADFGlobalConfig.STATUS_RUNNING,
                        ADFGlobalConfig.STATUS_SUBMITTED,
                        ADFGlobalConfig.STATUS_DELETING,
                    ]
                )
            ].to_list_of_dicts()
        ]
        if redo
        else state_handler.get_step_all(step)
    )


class ADFSequencer(ABC):
    def __init__(
        self,
        redo: bool = False,
    ):
        self.redo = redo

    @classmethod
    @abstractmethod
    def from_config(cls, config: Dict) -> "ADFSequencer":
        pass

    @abstractmethod
    def get_to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
    ) -> List[str]:
        pass

    def to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
    ) -> List[str]:
        return [
            str(batch_id)
            for batch_id in self.get_to_process(
                state_handler=state_handler, step_in=step_in, step_out=step_out
            )
            if (
                str(batch_id) not in get_ignore_list(state_handler, step_out, self.redo)
            )
        ]


class DefaultSequencer(ADFSequencer):
    @classmethod
    def from_config(cls, config: Dict) -> "DefaultSequencer":
        return cls(redo=config.get("redo", False))

    def get_to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
    ) -> List[str]:
        return [batch_id for batch_id in state_handler.get_step_success(step_in)]


class StrictIterativeSequencer(ADFSequencer):
    def __init__(
        self,
        batch_init: str,
        iterator: Optional[ADFFunction] = None,
        sort_key: Optional[ADFFunction] = None,
        redo: bool = False,
    ):
        super().__init__(redo=redo)
        self.batch_init = batch_init
        self.iterator = iterator or ADFFunction(
            "eval", {"expr": "lambda x: str(int(x) + 1)"}
        )
        self.sort_key = sort_key or ADFFunction(
            "eval", {"expr": "lambda x: str(int(x['batch_id']))"}
        )

    @classmethod
    def from_config(cls, config: Dict) -> "StrictIterativeSequencer":
        return cls(
            batch_init=extract_parameter(
                config, "batch_init", f"Loading {cls.__name__}"
            ),
            iterator=ADFFunction.from_config(config["iterator"])
            if "iterator" in config
            else None,
            sort_key=ADFFunction.from_config(config["sort_key"])
            if "sort_key" in config
            else None,
            redo=config.get("redo", False),
        )

    def get_to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
    ) -> List[str]:
        # Only run one batch at a time and when nothing is failed
        if (
            state_handler.get_step_submitted(step_out)
            + state_handler.get_step_running(step_out)
            + state_handler.get_step_failed(step_out)
        ):
            return []

        # Get list of processed batches
        processed = state_handler.get_entries(
            collection_name=step_out.flow.collection.name,
            flow_name=step_out.flow.name,
            step_name=step_out.name,
            version=step_out.version,
            layer=step_out.layer,
            status=ADFGlobalConfig.STATUS_SUCCESS,
        )

        # If nothing is processed, start with the initial batch
        if not processed:
            next_batch_id = self.batch_init
        # Otherwise, iterate on the latest batch
        else:
            next_batch_id = str(
                self.iterator(sorted(processed, key=self.sort_key)[-1]["batch_id"])
            )

        # Process the required batch only if it is available for input
        if next_batch_id in state_handler.get_step_success(step_in):
            return [next_batch_id]
        else:
            return []


class CronSequencer(ADFSequencer):
    def __init__(
        self,
        cron_expression: str,
        n_catchup: Optional[int] = None,
        start_from: Optional[str] = None,
        redo: bool = False,
    ):
        super().__init__(redo=redo)
        self.cron_expression = cron_expression
        self.n_catchup = n_catchup or 1
        self.start_from = (
            datetime.datetime.fromisoformat(start_from)
            if start_from is not None
            else None
        )
        if self.n_catchup < 1:
            raise ValueError(
                f"n_catchup must be strictly positive for {self.__class__.__name__}"
            )
        if (n_catchup is not None) and (start_from is not None):
            raise ValueError(
                f"Can't specify both n_catchup and start_from in {self.__class__.__name__}"
            )

    @classmethod
    def from_config(cls, config: Dict) -> "CronSequencer":
        return cls(
            cron_expression=extract_parameter(
                config, "cron_expression", f"Class {cls.__class__}"
            ),
            n_catchup=config.get("n_catchup", None),
            start_from=config.get("start_from", None),
            redo=config.get("redo", None),
        )

    def get_to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADF.components.flow_config.ADFStep",
        step_out: "ADF.components.flow_config.ADFStep",
    ) -> List[str]:
        batch_ids = []
        now = datetime.datetime.utcnow()
        if self.start_from is not None:
            cron = croniter.croniter(self.cron_expression, self.start_from)
            while True:
                next_time: datetime.datetime = cron.get_next(datetime.datetime)
                if next_time < now:
                    batch_ids.append(
                        next_time.strftime(ADFGlobalConfig.BATCH_TIME_FORMAT_STRING)
                    )
                else:
                    return batch_ids
        else:
            cron = croniter.croniter(self.cron_expression, now)
            for _ in range(0, self.n_catchup):
                batch_ids.append(
                    cron.get_prev(datetime.datetime).strftime(
                        ADFGlobalConfig.BATCH_TIME_FORMAT_STRING
                    )
                )
            return batch_ids


class ADFCombinationSequencer(ABC):
    def __init__(
        self,
        redo: bool = False,
    ):
        self.redo = redo

    @classmethod
    @abstractmethod
    def from_config(cls, config: Dict) -> "ADFCombinationSequencer":
        pass

    @abstractmethod
    def get_to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        combination_step: "ADF.components.flow_config.ADFCombinationStep",
    ) -> List[Tuple[List[str], str]]:
        pass

    def to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        combination_step: "ADF.components.flow_config.ADFCombinationStep",
    ) -> List[Tuple[List[str], str]]:
        ret = self.get_to_process(
            state_handler=state_handler,
            combination_step=combination_step,
        )
        for batch in ret:
            if len(batch[0]) != len(combination_step.input_steps):
                raise CombinationStepError(
                    step=combination_step,
                    context="SEQUENCING",
                    msg=f"Mismatched arg length (config : {len(combination_step.input_steps)}, provided : {len(batch[0])}).",
                )
        return [
            ([str(batch_id) for batch_id in batch[0]], str(batch[1]))
            for batch in ret
            if batch[1]
            not in get_ignore_list(state_handler, combination_step, self.redo)
        ]


class DefaultCombinationSequencer(ADFCombinationSequencer):
    @classmethod
    def from_config(cls, config: Dict) -> "DefaultCombinationSequencer":
        return cls(redo=config.get("redo", False))

    def get_to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        combination_step: "ADF.components.flow_config.ADFCombinationStep",
    ) -> List[Tuple[List[str], str]]:
        all_batches = []
        for arg_step in combination_step.input_steps:
            all_batches.append(set(state_handler.get_step_success(arg_step)))
        ready_batches = list(
            set.intersection(
                *[
                    set(state_handler.get_step_success(arg_step))
                    for arg_step in combination_step.input_steps
                ]
            )
        )
        return [
            (
                [batch_id for _ in combination_step.input_steps],
                batch_id,
            )
            for batch_id in ready_batches
        ]


class MonoBatchDispatchCombinationSequencer(ADFCombinationSequencer, ABC):
    def __init__(
        self,
        wrapped: ADFSequencer,
        redo: bool = False,
    ):
        super().__init__(redo=redo)
        self.wrapped = wrapped

    def get_to_process(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        combination_step: "ADF.components.flow_config.ADFCombinationStep",
    ) -> List[Tuple[List[str], str]]:
        return [
            (
                [batch_id for _ in combination_step.input_steps],
                batch_id,
            )
            for batch_id in self.wrapped.get_to_process(
                state_handler=state_handler,
                step_in=combination_step,
                step_out=combination_step,
            )
        ]

    @classmethod
    @abstractmethod
    def get_wrapped_class(cls) -> Type[ADFSequencer]:
        pass

    @classmethod
    def from_config(cls, config: Dict) -> "MonoBatchDispatchCombinationSequencer":
        return cls(
            wrapped=cls.get_wrapped_class().from_config(config),
            redo=config.get("redo", False),
        )


class CronCombinationSequencer(MonoBatchDispatchCombinationSequencer):
    @classmethod
    def get_wrapped_class(cls) -> Type[CronSequencer]:
        return CronSequencer

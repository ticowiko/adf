import subprocess

from typing import List, Optional
from abc import ABC, abstractmethod

import ADF
from ADF.components.data_structures import AbstractDataStructure
from ADF.components.flow_config import ADFStep, ADFCombinationStep


class AbstractDataInterface(ABC):
    @abstractmethod
    def read_batch_data(self, step: ADFStep, batch_id: str) -> AbstractDataStructure:
        pass

    @abstractmethod
    def read_full_data(self, step: ADFStep) -> AbstractDataStructure:
        pass

    def read_batches_data(
        self, step: ADFStep, batch_ids: List[str]
    ) -> Optional[AbstractDataStructure]:
        if not batch_ids:
            return None
        data = self.read_batch_data(step, batch_ids[0])
        return data.union(
            *[self.read_batch_data(step, batch_id) for batch_id in batch_ids[1:]]
        )

    @abstractmethod
    def write_batch_data(
        self, ads: AbstractDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        pass

    @abstractmethod
    def delete_step(self, step: ADFStep) -> None:
        pass

    @abstractmethod
    def delete_batch(self, step: ADFStep, batch_id: str) -> None:
        pass


class AbstractDataLayer(AbstractDataInterface, ABC):
    def __init__(self, as_layer: str):
        self.as_layer = as_layer

    @abstractmethod
    def setup_layer(self) -> None:
        pass

    @abstractmethod
    def setup_steps(self, steps: List[ADFStep]) -> None:
        pass

    @abstractmethod
    def detect_batches(self, step: ADFStep) -> List[str]:
        pass

    def __str__(self):
        return f"{self.__class__.__name__}:{self.as_layer}"

    def destroy(self) -> None:
        raise NotImplementedError

    def get_submit_args(
        self,
        step_in: ADFStep,
        step_out: ADFStep,
        batch_id: str,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
    ) -> List[str]:
        return [
            "python",
            implementer.get_exe_path(),
            icp,
            "apply-step",
            fcp,
            step_in.flow.name,
            step_in.name,
            step_out.flow.name,
            step_out.name,
            batch_id,
        ]

    def skip_submit(self) -> bool:
        return False

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
        if synchronous:
            implementer.apply_step(step_in, step_out, batch_id)
            return None
        else:
            return subprocess.Popen(
                self.get_submit_args(
                    step_in, step_out, batch_id, implementer, icp, fcp
                ),
                stdout=open(implementer.get_log_path(step_out, batch_id), "w"),
                stderr=open(implementer.get_err_path(step_out, batch_id), "w"),
            )

    def get_submit_combination_args(
        self,
        combination_step: ADFStep,
        batch_args: List[str],
        batch_id: str,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
    ) -> List[str]:
        return [
            "python",
            implementer.get_exe_path(),
            icp,
            "apply-combination-step",
            fcp,
            combination_step.flow.name,
            combination_step.name,
            batch_id,
            *batch_args,
        ]

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
        if synchronous:
            implementer.apply_combination_step(combination_step, batch_args, batch_id)
            return None
        else:
            return subprocess.Popen(
                self.get_submit_combination_args(
                    combination_step, batch_args, batch_id, implementer, icp, fcp
                ),
                stdout=open(implementer.get_log_path(combination_step, batch_id), "w"),
                stderr=open(implementer.get_err_path(combination_step, batch_id), "w"),
            )

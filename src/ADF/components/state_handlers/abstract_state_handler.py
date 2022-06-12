from typing import List, Optional, Dict, Tuple
from abc import ABC, abstractmethod

from ADF.components.flow_config import ADFStep, ADFCollection, AbstractDataFlow
from ADF.components.data_structures import AbstractDataStructure
from ADF.exceptions import UnknownBatch, CorruptedBatch
from ADF.config import ADFGlobalConfig


class AbstractStateHandler(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def set_status(
        self, step: ADFStep, batch_id: str, status: str, msg: Optional[str] = None
    ):
        pass

    @abstractmethod
    def destroy(self):
        pass

    @abstractmethod
    def delete_entry(
        self,
        step: ADFStep,
        batch_id: str,
    ):
        pass

    def set_submitted(
        self, step: ADFStep, batch_id: str, msg: Optional[str] = None
    ) -> None:
        self.set_status(
            step=step,
            batch_id=batch_id,
            status=ADFGlobalConfig.STATUS_SUBMITTED,
            msg=msg,
        )

    def set_running(
        self, step: ADFStep, batch_id: str, msg: Optional[str] = None
    ) -> None:
        self.set_status(
            step=step,
            batch_id=batch_id,
            status=ADFGlobalConfig.STATUS_RUNNING,
            msg=msg,
        )

    def set_deleting(
        self, step: ADFStep, batch_id: str, msg: Optional[str] = None
    ) -> None:
        self.set_status(
            step=step,
            batch_id=batch_id,
            status=ADFGlobalConfig.STATUS_DELETING,
            msg=msg,
        )

    def set_failed(
        self, step: ADFStep, batch_id: str, msg: Optional[str] = None
    ) -> None:
        self.set_status(
            step=step,
            batch_id=batch_id,
            status=ADFGlobalConfig.STATUS_FAILED,
            msg=msg,
        )

    def set_success(
        self, step: ADFStep, batch_id: str, msg: Optional[str] = None
    ) -> None:
        self.set_status(
            step=step,
            batch_id=batch_id,
            status=ADFGlobalConfig.STATUS_SUCCESS,
            msg=msg,
        )

    # TODO : impose columns : collection_name, flow_name, step_name, version, layer, batch_id, status, datetime, msg
    @abstractmethod
    def to_ads(self) -> AbstractDataStructure:
        pass

    def to_collection_ads(self, flows: ADFCollection) -> AbstractDataStructure:
        ads = self.to_ads()
        return ads[ads["collection_name"] == flows.name]

    def to_flow_ads(self, flow: AbstractDataFlow) -> AbstractDataStructure:
        ads = self.to_ads()
        return ads[
            (ads["collection_name"] == flow.collection.name)
            & (ads["flow_name"] == flow.name)
        ]

    def to_step_ads(self, step: ADFStep) -> AbstractDataStructure:
        ads = self.to_ads()
        return ads[
            (ads["collection_name"] == step.flow.collection.name)
            & (ads["flow_name"] == step.flow.name)
            & (ads["step_name"] == step.name)
            & (ads["version"] == step.version)
        ]

    # TODO : impose column types
    def get_entries(
        self,
        collection_name: Optional[str] = None,
        flow_name: Optional[str] = None,
        step_name: Optional[str] = None,
        version: Optional[str] = None,
        layer: Optional[str] = None,
        batch_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict]:
        search_args: List[Dict[str, str]] = [
            {"col": key, "val": val}
            for key, val in {
                "collection_name": collection_name,
                "flow_name": flow_name,
                "step_name": step_name,
                "version": version,
                "layer": layer,
                "batch_id": batch_id,
                "status": status,
            }.items()
            if val is not None
        ]
        ads = self.to_ads()
        if not search_args:
            return ads.to_list_of_dicts()
        condition = ads[search_args[0]["col"]] == search_args[0]["val"]
        for search_arg in search_args[1:]:
            condition = condition & (ads[search_arg["col"]] == search_arg["val"])
        return ads[condition].to_list_of_dicts()

    def get_step_per_status(self, step: ADFStep, status: str) -> List[str]:
        return [
            row["batch_id"]
            for row in self.get_entries(
                collection_name=step.flow.collection.name,
                flow_name=step.flow.name,
                step_name=step.name,
                version=step.version,
                layer=step.layer,
                status=status,
            )
        ]

    def get_step_submitted(self, step: ADFStep) -> List[str]:
        return self.get_step_per_status(step, ADFGlobalConfig.STATUS_SUBMITTED)

    def get_step_running(self, step: ADFStep) -> List[str]:
        return self.get_step_per_status(step, ADFGlobalConfig.STATUS_RUNNING)

    def get_step_deleting(self, step: ADFStep) -> List[str]:
        return self.get_step_per_status(step, ADFGlobalConfig.STATUS_DELETING)

    def get_step_failed(self, step: ADFStep) -> List[str]:
        return self.get_step_per_status(step, ADFGlobalConfig.STATUS_FAILED)

    def get_step_success(self, step: ADFStep) -> List[str]:
        return self.get_step_per_status(step, ADFGlobalConfig.STATUS_SUCCESS)

    def get_step_all(self, step) -> List[str]:
        return [
            row["batch_id"]
            for row in self.get_entries(
                collection_name=step.flow.collection.name,
                flow_name=step.flow.name,
                step_name=step.name,
                version=step.version,
                layer=step.layer,
            )
        ]

    def get_batch_info(self, step: ADFStep, batch_id: str) -> Dict:
        rows = self.get_entries(
            collection_name=step.flow.collection.name,
            flow_name=step.flow.name,
            step_name=step.name,
            version=step.version,
            layer=step.layer,
            batch_id=batch_id,
        )
        if len(rows) == 1:
            return rows[0]
        elif len(rows) == 0:
            raise UnknownBatch(step, batch_id)
        else:
            raise CorruptedBatch(step, batch_id)

    def get_batch_status(self, step: ADFStep, batch_id: str) -> str:
        return self.get_batch_info(step, batch_id)["status"]

    def get_downstream(
        self, step: ADFStep, batch_ids: List[str]
    ) -> List[Tuple[ADFStep, List[str]]]:
        downstream: List[Tuple[ADFStep, List[str]]] = [(step, batch_ids)]
        for downstream_step in step.get_downstream_steps():
            next_batch_ids: List[str] = []
            for batch_id in batch_ids:
                next_batch_ids += downstream_step.get_batch_dependency(
                    state_handler=self,
                    step_in=step,
                    batch_id=batch_id,
                )
            next_batch_ids = list(set(next_batch_ids))
            downstream += self.get_downstream(downstream_step, next_batch_ids)
        return downstream

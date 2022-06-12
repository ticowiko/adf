from abc import abstractmethod

from typing import List, Type

from ADF.components.data_structures import AbstractDataStructure
from ADF.components.layers import AbstractDataLayer, LocalFileDataLayer
from ADF.components.flow_config import ADFStep
from ADF.components.layer_transitions import ADLTransition


class TransitionLocalToLocal(ADLTransition):
    def default_to_write_out(self) -> bool:
        return True

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [LocalFileDataLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [LocalFileDataLayer]

    def write_batch_data(
        self, ads: AbstractDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.layer_in: LocalFileDataLayer
        self.layer_out: LocalFileDataLayer
        _, data = self.layer_in.dump_ads(ads)
        self.layer_out.write_meta_data(step, batch_id, {}, data)

    def read_batch_data(self, step: ADFStep, batch_id: str) -> AbstractDataStructure:
        self.layer_in: LocalFileDataLayer
        self.layer_out: LocalFileDataLayer
        ads = self.layer_in.read_batch_data(step, batch_id)
        _, data = self.layer_in.dump_ads(ads)
        return self.layer_out.read_meta_data(meta={}, data=data)

    def read_full_data(self, step: ADFStep) -> AbstractDataStructure:
        self.layer_in: LocalFileDataLayer
        self.layer_out: LocalFileDataLayer
        ads = self.layer_in.read_full_data(step)
        _, data = self.layer_in.dump_ads(ads)
        return self.layer_out.read_meta_data(meta={}, data=data)

    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def delete_step_write_out(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_step_read_in(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)

    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)

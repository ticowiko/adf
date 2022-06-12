from abc import ABC, abstractmethod
from typing import List, Type

from ADF.components.data_structures import AbstractDataStructure
from ADF.components.layers import AbstractDataLayer, AbstractDataInterface
from ADF.components.flow_config import ADFStep
from ADF.exceptions import ADLUnsupportedTransition


class ADLTransition(AbstractDataInterface, ABC):
    def __init__(
        self,
        layer_in: AbstractDataLayer,
        layer_out: AbstractDataLayer,
    ):
        self.layer_in = layer_in
        self.layer_out = layer_out
        self.validate()

    def __str__(self):
        return f"{self.__class__.__name__}::{str(self.layer_in)}->{str(self.layer_out)}"

    def validate(self) -> None:
        if not self.supports_transition(self.layer_in, self.layer_out):
            raise ADLUnsupportedTransition(self, "Not in handled classes.")

    def delete_step(self, step: ADFStep) -> None:
        if step.get_transition_write_out(self):
            self.delete_step_write_out(step)
        else:
            self.delete_step_read_in(step)

    def delete_batch(self, step: ADFStep, batch_id: str) -> None:
        if step.get_transition_write_out(self):
            self.delete_batch_write_out(step, batch_id)
        else:
            self.delete_batch_read_in(step, batch_id)

    @classmethod
    def supports_transition(
        cls, layer_in: AbstractDataLayer, layer_out: AbstractDataLayer
    ):
        return any(
            [isinstance(layer_in, layer) for layer in cls.get_handled_layers_in()]
        ) and any(
            [isinstance(layer_out, layer) for layer in cls.get_handled_layers_out()]
        )

    @abstractmethod
    def default_to_write_out(self) -> bool:
        pass

    @staticmethod
    @abstractmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        pass

    @staticmethod
    @abstractmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        pass

    @abstractmethod
    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    @abstractmethod
    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    @abstractmethod
    def delete_step_write_out(self, step: ADFStep):
        pass

    @abstractmethod
    def delete_step_read_in(self, step: ADFStep):
        pass

    @abstractmethod
    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        pass

    @abstractmethod
    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        pass


class TrivialTransition(ADLTransition):
    def validate(self) -> None:
        super().validate()
        if self.layer_in.as_layer != self.layer_out.as_layer:
            raise ADLUnsupportedTransition(
                self, "Trivial transition only applies to a layer and itself."
            )

    def default_to_write_out(self) -> bool:
        return False

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [AbstractDataLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [AbstractDataLayer]

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

    def read_batch_data(self, step: ADFStep, batch_id: str) -> AbstractDataStructure:
        return self.layer_in.read_batch_data(step, batch_id)

    def read_full_data(self, step: ADFStep) -> AbstractDataStructure:
        return self.layer_in.read_full_data(step)

    def write_batch_data(
        self, ads: AbstractDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.layer_out.write_batch_data(ads, step, batch_id)

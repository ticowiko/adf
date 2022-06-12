from typing import Optional, List

import ADF
from ADF.exceptions import (
    UnknownStep,
    InvalidAbstractDataFlow,
    InvalidADFStep,
)
from ADF.components.flow_config import ADFStep, ADFStartingStep
from ADF.utils import ToDictMixin


class AbstractDataFlow(ToDictMixin):
    flat_attrs = ["name", "collection_name"]
    iter_attrs = ["steps"]

    def __init__(
        self,
        name: str,
        steps: Optional[List[ADFStep]] = None,
        collection: Optional[
            "ADF.components.flow_config.collection.ADFCollection"
        ] = None,
    ):
        self.name = name
        self.steps = steps or []
        self.collection = collection

    def __hash__(self):
        return hash(f"{self.collection.name}/{self.name}")

    def __eq__(self, other: "AbstractDataFlow"):
        return hash(self) == hash(other)

    def get_dict_collection_name(self):
        return self.collection.name

    def add_step(self, step: ADFStep) -> None:
        self.steps.append(step)

    def get_step(self, step_name: str) -> ADFStep:
        for step in self.steps:
            if step.name == step_name:
                return step
        raise UnknownStep(self, step_name)

    def validate(self) -> None:
        errs = []
        seen = set()
        if "/" in self.name:
            errs.append(f"Flow name '{self.name}' contains '/'.")
        if not isinstance(self.steps[0], ADFStartingStep):
            errs.append(f"First step of flow '{self.name}' is not a starting step.")
        for step in self.steps[1:]:
            if isinstance(step, ADFStartingStep):
                errs.append(
                    f"Step {str(step)} of flow {self.name} is a starting step (and shouldn't be)."
                )
        for step in self.steps:
            try:
                step.validate()
            except InvalidADFStep as e:
                errs += e.errs
            if step.name in seen:
                errs.append(f"Step '{step.name}' duplicated in flow {self.name}.")
            seen.add(step.name)
        if errs:
            raise InvalidAbstractDataFlow(errs)

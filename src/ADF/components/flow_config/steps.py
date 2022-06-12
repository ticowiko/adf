from abc import ABC, abstractmethod
from typing import Callable, Optional, List, Dict, Type, Union
from importlib import import_module

import ADF
from ADF.exceptions import (
    InvalidADFStep,
    UnknownFlow,
    NoPreviousStep,
    NoNextStep,
    InvalidInputStep,
)
from ADF.components.flow_config import (
    ADFFunction,
    ADFModule,
    ADFMetaHandler,
    ADFSequencer,
    ADFCombinationSequencer,
    DefaultSequencer,
    DefaultCombinationSequencer,
    ADFBatchDependencyHandler,
    DefaultBatchDependencyHandler,
    ADFDataLoader,
    DefaultDataLoader,
    NullDataLoader,
)
from ADF.components.data_structures import AbstractDataStructure
from ADF.utils import ToDictMixin, extract_parameter


def get_flow_from_flows(
    flows: List["ADF.components.flow_config.AbstractDataFlow"], flow_name: str
) -> "ADF.components.flow_config.AbstractDataFlow":
    for flow in flows:
        if flow.name == flow_name:
            return flow
    raise UnknownFlow(flow_name)


def load_class(config):
    loaded_class = getattr(
        import_module(ADFModule.modules[config["module"]].import_path),
        config["class_name"],
    )
    return loaded_class.from_config(config.get("params", {}))


class ADFStep(ToDictMixin):
    flat_attrs = [
        "collection_name",
        "flow_name",
        "name",
        "version",
        "layer",
        "transition_write_out",
        "step_id",
        "upstream_step_ids",
    ]
    deep_attrs = ["meta"]

    def __init__(
        self,
        flow: "ADF.components.flow_config.AbstractDataFlow",
        name: str,
        func: Optional[ADFFunction] = None,
        func_kwargs: Optional[Dict] = None,
        version: str = "default",
        layer: str = "default",
        meta: Optional[ADFMetaHandler] = None,
        sequencer: Optional[Union[ADFSequencer, ADFCombinationSequencer]] = None,
        batch_dependency: Optional[ADFBatchDependencyHandler] = None,
        transition_write_out: Optional[bool] = None,
        data_loader: Optional[ADFDataLoader] = None,
        attach: bool = False,
    ):
        self.flow = flow
        self.name = name
        self.func = func
        self.func_kwargs: Dict = func_kwargs or {}
        self.version = version
        self.layer = layer
        self.meta: ADFMetaHandler = meta or ADFMetaHandler()
        self.sequencer: Union[ADFSequencer, ADFCombinationSequencer] = (
            sequencer or DefaultSequencer()
        )
        self.batch_dependency = batch_dependency or DefaultBatchDependencyHandler()
        self.transition_write_out = transition_write_out
        self.data_loader: ADFDataLoader = data_loader or DefaultDataLoader()
        if attach:
            self.flow.add_step(self)

    def __hash__(self):
        return hash(
            f"{self.flow.collection.name}/{self.flow.name}/{self.name}/{self.version}/{self.layer}"
        )

    def __eq__(self, other: "ADFStep"):
        return hash(self) == hash(other)

    def get_dict_collection_name(self):
        return self.flow.collection.name

    def get_dict_flow_name(self):
        return self.flow.name

    def get_dict_step_id(self):
        return f"{self.flow.collection.name}/{self.flow.name}/{self.name}/{self.version}/{self.layer}"

    def get_dict_upstream_step_ids(self):
        return [step.get_dict_step_id() for step in self.get_upstream_steps()]

    def get_upstream_steps(self) -> List["ADFStep"]:
        try:
            return [self.get_previous_step()]
        except NoPreviousStep:
            return []

    def get_downstream_steps(self) -> List["ADFStep"]:
        downstream: List["ADFStep"] = []
        try:
            downstream.append(self.get_next_step())
        except NoNextStep:
            pass
        downstream += self.get_output_steps()
        downstream += self.get_downstream_combination_steps()
        return downstream

    def get_output_steps(self) -> List["ADFReceptionStep"]:
        return self.flow.collection.get_output_steps(self)

    def get_downstream_combination_steps(self) -> List["ADFCombinationStep"]:
        return self.flow.collection.get_downstream_combination_steps(self)

    def get_batch_dependency(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADFStep",
        batch_id: str,
    ) -> List[str]:
        return self.batch_dependency.dependent_batches(
            state_handler=state_handler,
            step_in=step_in,
            step_out=self,
            batch_id=batch_id,
        )

    def validate(self) -> None:
        errs = []
        if "/" in self.name:
            errs.append(f"Step name of {str(self)} contains '/'.")
        if "/" in self.version:
            errs.append(f"Version name of {str(self)} contains '/'.")
        if "/" in self.layer:
            errs.append(f"Layer name of {str(self)} contains '/'.")
        if not isinstance(self.sequencer, ADFSequencer) and not isinstance(
            self, ADFCombinationStep
        ):
            errs.append(
                f"Sequencer for non combination step {str(self)} does not subclass '{ADFSequencer.__name__}'."
            )
        if not isinstance(self.batch_dependency, ADFBatchDependencyHandler):
            errs.append(
                f"Batch dependency handler for step {str(self)} does not subclass '{ADFBatchDependencyHandler.__name__}'."
            )
        if (self.func is None) and self.func_kwargs:
            errs.append(f"Step {str(self)} has function kwargs but no function.")
        try:
            previous_step = self.get_previous_step()
            if (self.transition_write_out is not None) and (
                previous_step.layer == self.layer
            ):
                errs.append(
                    f"Step {str(self)} has set transition preference but is in same layer as previous step {previous_step}."
                )
            if (
                (self.data_loader.__class__ is not DefaultDataLoader)
                and (previous_step.layer != self.layer)
                and (self.transition_write_out is not True)
            ):
                errs.append(
                    f"Step {str(self)} : to define a custom data loader over a transition you must impose 'transition_write_out' as true."
                )
        except NoPreviousStep:
            if self.transition_write_out is not None:
                errs.append(
                    f"Step {str(self)} has set transition preference but has no previous step."
                )
        output_steps = self.get_output_steps()
        if output_steps:
            try:
                next_step = self.get_next_step()
                errs.append(
                    f"Step {str(self)} is hooked to output steps \
                '{', '.join([str(output_step) for output_step in output_steps])}', and yet it has a next step \
                '{str(next_step)}'."
                )
            except NoNextStep:
                pass
            downstream_combination_steps = self.get_downstream_combination_steps()
            if downstream_combination_steps:
                errs.append(
                    f"Step {str(self)} is hooked to output steps \
                    '{', '.join(str(output_steps))}', and yet it has downstream combination steps \
                    '{', '.join([str(combination_step) for combination_step in downstream_combination_steps])}'."
                )
            if self.meta.columns:
                errs.append(
                    f"Step {str(self)} is hooked to output steps yet defines custom meta columns."
                )
        if errs:
            raise InvalidADFStep(errs)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"{self.layer}:{self.flow.name}:{self.name}"

    def apply_batch_function(
        self,
        ads_args: List[AbstractDataStructure],
        ads_kwargs: Dict[str, AbstractDataStructure],
    ) -> Union[AbstractDataStructure, Dict[str, AbstractDataStructure]]:
        if self.func is None and len(ads_args) == 1 and len(ads_kwargs.keys()) == 0:
            return ads_args[0]
        elif self.func is None:
            raise ValueError(
                f"Cannot interpret undefined function for non trivial inputs in step '{str(self)}'."
            )
        else:
            return self.func(*ads_args, **ads_kwargs, **self.func_kwargs)

    def to_arg(self) -> str:
        return f"{self.name}/{self.flow.name}"

    def get_transition_write_out(
        self, transition: "ADF.components.layer_transitions.ADLTransition"
    ) -> bool:
        return self.transition_write_out is True or (
            (self.transition_write_out is None) and transition.default_to_write_out()
        )

    def get_partition_key(self) -> List[str]:
        return (
            sorted([col.name for col in self.meta.columns if col.in_partition])
            if self.meta
            else []
        )

    def get_previous_step(self) -> "ADFStep":
        for previous_step, current_step in zip(
            self.flow.steps[:-1], self.flow.steps[1:]
        ):
            if current_step.name == self.name:
                return previous_step
        raise NoPreviousStep(self)

    def get_next_step(self) -> "ADFStep":
        for current_step, next_step in zip(self.flow.steps[:-1], self.flow.steps[1:]):
            if current_step.name == self.name:
                return next_step
        raise NoNextStep(self)

    @staticmethod
    def from_config(
        step_config: Dict,
        flow: "ADF.components.flow_config.AbstractDataFlow",
        flows: List["ADF.components.flow_config.AbstractDataFlow"],
        attach: bool = True,
    ):
        start_mapping: Dict[str, Type[ADFStep]] = {
            "landing": ADFLandingStep,
            "combination": ADFCombinationStep,
            "reception": ADFReceptionStep,
        }
        params = {}
        if "start" in step_config:
            step_class = start_mapping[step_config["start"]]
            if step_class is ADFCombinationStep:
                params = {
                    "input_steps": [
                        get_flow_from_flows(
                            flows,
                            extract_parameter(
                                arg_step,
                                "flow_name",
                                f"input step of combination step '{flow.name}/{extract_parameter(step_config, 'name', f'unknown step in flow {flow.name}')}'",
                            ),
                        ).get_step(
                            extract_parameter(
                                arg_step,
                                "step_name",
                                f"input step of combination step '{flow.name}/{extract_parameter(step_config, 'name', f'unknown step in flow {flow.name}')}'",
                            )
                        )
                        for arg_step in extract_parameter(
                            step_config,
                            "input_steps",
                            f"step '{flow.name}/{extract_parameter(step_config, 'name', f'unknown step in flow {flow.name}')}'",
                        )
                    ],
                    "data_loaders": [
                        DefaultDataLoader()
                        if "data_loader" not in arg_step
                        else load_class(arg_step["data_loader"])
                        for arg_step in extract_parameter(
                            step_config,
                            "input_steps",
                            f"step '{flow.name}/{extract_parameter(step_config, 'name', f'unknown step in flow {flow.name}')}'",
                        )
                    ],
                    "batch_dependencies": [
                        DefaultBatchDependencyHandler()
                        if "batch_dependency" not in arg_step
                        else load_class(arg_step["batch_dependency"])
                        for arg_step in extract_parameter(
                            step_config,
                            "input_steps",
                            f"step '{flow.name}/{extract_parameter(step_config, 'name', f'unknown step in flow {flow.name}')}'",
                        )
                    ],
                }
            elif step_class is ADFReceptionStep:
                step_info = f"{flow.name}/{extract_parameter(step_config, 'name', f'unknown step in flow {flow.name}')}"
                input_steps = []
                for input_step_config in extract_parameter(
                    step_config,
                    "input_steps",
                    f"step config of reception step {step_info}",
                ):
                    input_steps.append(
                        get_flow_from_flows(
                            flows,
                            extract_parameter(
                                input_step_config,
                                "flow_name",
                                f"input step of reception step '{step_info}'",
                            ),
                        ).get_step(
                            extract_parameter(
                                input_step_config,
                                "step_name",
                                f"input step of reception step '{step_info}'",
                            )
                        )
                    )
                params = {
                    "input_steps": input_steps,
                    "key": extract_parameter(
                        step_config, "key", f"step config of reception step {step_info}"
                    ),
                }
        else:
            step_class = ADFStep
        step_class(
            flow=flow,
            func=ADFFunction.from_config(step_config["func"])
            if "func" in step_config
            else None,
            func_kwargs=step_config.get("func_kwargs"),
            meta=ADFMetaHandler.from_config(step_config["meta"])
            if "meta" in step_config
            else None,
            sequencer=load_class(step_config["sequencer"])
            if "sequencer" in step_config
            else None,
            batch_dependency=load_class(step_config["batch_dependency"])
            if "batch_dependency" in step_config
            else None,
            attach=attach,
            transition_write_out=step_config.get("transition_write_out"),
            data_loader=load_class(step_config["data_loader"])
            if "data_loader" in step_config
            else None,
            **{
                key: extract_parameter(
                    step_config,
                    key,
                    f"step '{flow.name}/{extract_parameter(step_config, 'name', f'unknown step in flow {flow.name}')}'",
                )
                for key in step_config
                if key in ["name", "version", "layer"]
            },
            **params,
        )


class ADFStartingStep(ADFStep, ABC):
    @abstractmethod
    def orchestrate_start(
        self,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ):
        pass

    @abstractmethod
    def get_upstream_steps(self) -> List["ADFStep"]:
        pass


class ADFReceptionStep(ADFStartingStep):
    def __init__(
        self,
        flow: "ADF.components.flow_config.AbstractDataFlow",
        name: str,
        input_steps: List[ADFStep],
        key: str,
        func: Optional[ADFFunction] = None,
        func_kwargs: Optional[Dict] = None,
        version: str = "default",
        layer: str = "default",
        meta: Optional[ADFMetaHandler] = None,
        sequencer: Optional[Union[ADFSequencer, ADFCombinationSequencer]] = None,
        batch_dependency: Optional[ADFBatchDependencyHandler] = None,
        transition_write_out: Optional[bool] = None,
        data_loader: Optional[ADFDataLoader] = None,
        attach: bool = False,
    ):
        super().__init__(
            flow=flow,
            name=name,
            func=func,
            func_kwargs=func_kwargs,
            version=version,
            layer=layer,
            meta=meta,
            sequencer=sequencer,
            batch_dependency=batch_dependency,
            transition_write_out=transition_write_out,
            data_loader=data_loader,
            attach=attach,
        )
        self.input_steps = input_steps
        self.key = key

    def validate(self) -> None:
        errs = []
        try:
            super().validate()
        except InvalidADFStep as e:
            errs += e.errs
        if self.func is not None:
            errs.append(f"Reception step {str(self)} has non null function.")
        if self.func_kwargs:
            errs.append(f"Reception step {str(self)} has non null function kwargs.")
        if self.sequencer.__class__ is not DefaultSequencer:
            errs.append(f"Reception step {str(self)} has custom sequencer.")
        if self.batch_dependency.__class__ is not DefaultBatchDependencyHandler:
            errs.append(f"Reception step {str(self)} has custom batch dependency.")
        if self.transition_write_out is not None:
            errs.append(
                f"Reception step {str(self)} cannot impose transition direction."
            )
        if self.data_loader.__class__ is not DefaultDataLoader:
            errs.append(f"Reception step {str(self)} has custom data loader.")
        for input_step in self.input_steps:
            if isinstance(input_step, ADFLandingStep):
                errs.append(
                    f"Landing step {str(input_step)} hooked to reception step {str(self)}."
                )
        if len(self.input_steps) > 1:
            # TODO : find solution to identical batch names from different input steps
            # TODO : change default reception step batch dependency to match solution
            # TODO : one option is to append step identifiers to batch ids
            # TODO : using the full step name is too long :
            # TODO : OPTION 1 : add a batch prefix option to reception steps
            # TODO : OPTION 2 : make the reception step alias change per input step
            errs.append(
                f"Multiple inputs to reception step {str(self)}, this is not yet supported."
            )
        if not self.input_steps:
            errs.append(f"Reception step {str(self)} has no input steps.")
        # TODO : how to make versioning safe for a reception step, especially with multiple input steps ?
        if self.version != "default" or self.version != self.input_steps[0].version:
            errs.append(
                f"Reception step {str(self)} has specified an impossible version."
            )
        self.version = self.input_steps[0].version
        if errs:
            raise InvalidADFStep(errs)

    def orchestrate_start(
        self,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ):
        pass

    def get_upstream_steps(self) -> List["ADFStep"]:
        return self.input_steps


class ADFLandingStep(ADFStartingStep):
    def __init__(
        self,
        flow: "ADF.components.flow_config.AbstractDataFlow",
        name: str,
        func: Optional[Callable] = None,
        func_kwargs: Optional[Dict] = None,
        version: str = "default",
        layer: str = "default",
        meta: Optional[ADFMetaHandler] = None,
        sequencer: Optional[ADFSequencer] = None,
        batch_dependency: Optional[ADFBatchDependencyHandler] = None,
        transition_write_out: Optional[bool] = None,
        data_loader: Optional[ADFDataLoader] = None,
        attach: bool = False,
    ):
        super().__init__(
            flow=flow,
            name=name,
            func=func,
            func_kwargs=func_kwargs,
            version=version,
            layer=layer,
            meta=meta,
            sequencer=sequencer,
            batch_dependency=batch_dependency,
            transition_write_out=transition_write_out,
            data_loader=data_loader,
            attach=attach,
        )

    def get_upstream_steps(self) -> List["ADFStep"]:
        return []

    def orchestrate_start(
        self,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ):
        implementer.handle_landing(self)

    def validate(self) -> None:
        errs = []
        try:
            super().validate()
        except InvalidADFStep as e:
            errs += e.errs
        if self.func is not None:
            errs.append(f"Landing step {str(self)} has non null function.")
        if self.func_kwargs:
            errs.append(f"Landing step {str(self)} has non null function kwargs.")
        if self.sequencer.__class__ is not DefaultSequencer:
            errs.append(f"Landing step {str(self)} has custom sequencer.")
        if self.batch_dependency.__class__ is not DefaultBatchDependencyHandler:
            errs.append(f"Landing step {str(self)} has custom batch dependency.")
        if self.transition_write_out is not None:
            errs.append(f"Landing step {str(self)} cannot impose transition direction.")
        if self.data_loader.__class__ is not DefaultDataLoader:
            errs.append(f"Landing step {str(self)} has custom data loader.")
        if self.transition_write_out is not None:
            errs.append(f"Landing step {str(self)} imposes transition direction.")
        if errs:
            raise InvalidADFStep(errs)


class ADFCombinationStep(ADFStartingStep):
    def __init__(
        self,
        flow: "ADF.components.flow_config.AbstractDataFlow",
        name: str,
        input_steps: List[ADFStep],
        func: Optional[ADFFunction] = None,
        func_kwargs: Optional[Dict] = None,
        version: str = "default",
        layer: str = "default",
        meta: Optional[ADFMetaHandler] = None,
        sequencer: Optional[ADFCombinationSequencer] = None,
        batch_dependency: Optional[ADFBatchDependencyHandler] = None,
        batch_dependencies: Optional[List[ADFBatchDependencyHandler]] = None,
        transition_write_out: Optional[bool] = None,
        data_loader: Optional[ADFDataLoader] = None,
        data_loaders: Optional[List[ADFDataLoader]] = None,
        attach: bool = False,
    ):
        super().__init__(
            flow=flow,
            name=name,
            func=func,
            func_kwargs=func_kwargs,
            version=version,
            layer=layer,
            meta=meta,
            sequencer=sequencer or DefaultCombinationSequencer(),
            batch_dependency=batch_dependency,
            transition_write_out=transition_write_out,
            data_loader=data_loader or NullDataLoader(),
            attach=attach,
        )
        self.input_steps = input_steps
        self.batch_dependencies = batch_dependencies or [
            DefaultBatchDependencyHandler() for _ in input_steps
        ]
        self.data_loaders = data_loaders or [DefaultDataLoader() for _ in input_steps]

    def get_upstream_steps(self) -> List["ADFStep"]:
        return self.input_steps

    def get_batch_dependency(
        self,
        state_handler: "ADF.components.state_handlers.AbstractStateHandler",
        step_in: "ADFStep",
        batch_id: str,
    ) -> List[str]:
        matching_dependency = None
        for input_step, batch_dependency in zip(
            self.input_steps, self.batch_dependencies
        ):
            if input_step == step_in:
                matching_dependency = batch_dependency
        if matching_dependency is None:
            raise InvalidInputStep(input_step=step_in, combination_step=self)
        return matching_dependency.dependent_batches(
            state_handler=state_handler,
            step_in=step_in,
            step_out=self,
            batch_id=batch_id,
        )

    def orchestrate_start(
        self,
        implementer: "ADF.components.implementers.ADFImplementer",
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ):
        implementer.submit_combination_step(
            combination_step=self,
            icp=icp,
            fcp=fcp,
            synchronous=synchronous,
        )

    def validate(self) -> None:
        errs = []
        try:
            super().validate()
        except InvalidADFStep as e:
            errs += e.errs
        if not isinstance(self.sequencer, ADFCombinationSequencer):
            errs.append(
                f"Sequencer for combination step {str(self)} does not subclass '{ADFCombinationSequencer.__name__}'."
            )
        if self.batch_dependency.__class__ is not DefaultBatchDependencyHandler:
            errs.append(
                f"Found custom batch dependency for combination step {str(self)}, set batch dependency per input step."
            )
        if self.func is None:
            if len(self.input_steps) != 1:
                errs.append(
                    f"In step {str(self)} can't have multiple input steps to combination step and no defined function."
                )
        if len(self.data_loaders) != len(self.input_steps):
            errs.append(
                f"Insufficient number of data loaders given input steps (input steps : {len(self.input_steps)}, data loaders : {len(self.data_loaders)})"
            )
        for input_step in self.input_steps:
            output_steps = input_step.get_output_steps()
            if output_steps:
                errs.append(
                    f"Combination step {str(self)} has input step {str(input_step)} hooked to output steps {[str(output_step) for output_step in output_steps]}"
                )
        for input_step, data_loader, batch_dependency in zip(
            self.input_steps, self.data_loaders, self.batch_dependencies
        ):
            if not isinstance(data_loader, ADFDataLoader):
                errs.append(
                    f"Combination step {str(self)} has input step {str(input_step)} with invalid data loader type '{data_loader.__class__.__name__}'."
                )
            if not isinstance(batch_dependency, ADFBatchDependencyHandler):
                errs.append(
                    f"Combination step {str(self)} has input step {str(input_step)} with invalid batch dependency type '{batch_dependency.__class__.__name__}'."
                )
        if errs:
            raise InvalidADFStep(errs)

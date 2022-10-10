import os
import sys
import yaml
import shutil
import logging
import datetime
import subprocess

from signal import SIGINT
from copy import deepcopy
from inspect import isclass
from importlib import import_module
from argparse import ArgumentParser
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Type, TextIO, Union

from ADF.components.data_structures import AbstractDataStructure
from ADF.components.layers import AbstractDataInterface, AbstractDataLayer
from ADF.components.flow_config import (
    ADFStep,
    ADFReceptionStep,
    ADFLandingStep,
    ADFCombinationStep,
    ADFCollection,
)
from ADF.components.implementers import (
    Subcommand,
    SetupImplementerSubcommand,
    UpdateCodeSubcommand,
    SetupFlowsSubcommand,
    ApplyStepSubCommand,
    ApplyCombinationStepSubCommand,
    OrchestrateSubcommand,
    ResetBatchesSubcommand,
    OutputPrebuiltConfigSubcommand,
)
from ADF.components.state_handlers import AbstractStateHandler
from ADF.components.layer_transitions import ADLTransition, TrivialTransition
from ADF.exceptions import (
    ADLUnhandledTransition,
    CombinationStepError,
)
from ADF.config import ADFGlobalConfig
from ADF.utils import s3_client, s3_url_to_bucket_and_key, run_command


class ADFImplementer(ABC):
    _exe_path: Optional[str] = None
    _class_key = "implementer_class"
    _pkgs_key = "extra_packages"

    @classmethod
    def get_exe_name(cls) -> str:
        return "adf-launcher.py"

    @classmethod
    def get_exe_path(cls):
        if cls._exe_path is None:
            cls._exe_path = shutil.which(cls.get_exe_name())
        if cls._exe_path is None:
            raise RuntimeError(
                f"Failed to find exe path for {cls.__name__} given exe name {cls.get_exe_name()}"
            )
        return cls._exe_path

    def __init__(
        self,
        layers: Dict[str, AbstractDataLayer],
        state_handler: AbstractStateHandler,
        transitions: Optional[List[Type[ADLTransition]]] = None,
        transition_matrix: Optional[
            Dict[Tuple[str, str], Optional[Type[ADLTransition]]]
        ] = None,
        extra_packages: Optional[List[str]] = None,
    ):
        os.chdir(ADFGlobalConfig.ADF_WORKING_DIR)
        self.layers = layers
        self.state_handler = state_handler
        self.extra_packages: List[str] = (
            []
            if extra_packages is None
            else [os.path.abspath(extra_package) for extra_package in extra_packages]
        )
        default_transition_matrix: Dict[Tuple[str, str], None] = {
            (layer_in, layer_out): None for layer_in in layers for layer_out in layers
        }
        self.transition_matrix: Dict[Tuple[str, str], Optional[Type[ADLTransition]]] = (
            transition_matrix or default_transition_matrix
        )
        if set(self.transition_matrix.keys()) != set(default_transition_matrix.keys()):
            raise ValueError(
                f"Key mismatch in custom transition matrix, required : {set(default_transition_matrix.keys())}, given: {set(self.transition_matrix.keys())}"
            )
        if transitions is not None:
            for layer_in, layer_out in self.transition_matrix:
                if self.transition_matrix[(layer_in, layer_out)] is None:
                    for transition in transitions:
                        if transition.supports_transition(
                            self.layers[layer_in], self.layers[layer_out]
                        ):
                            self.transition_matrix[(layer_in, layer_out)] = transition
                            break
                if layer_in == layer_out:
                    self.transition_matrix[(layer_in, layer_out)] = TrivialTransition
        self.subprocesses: Dict[Tuple[ADFStep, str], subprocess.Popen] = {}

    def add_packages(self, extra_packages: List[str]):
        self.extra_packages += [
            os.path.abspath(extra_package) for extra_package in extra_packages
        ]

    def set_packages(self, extra_packages: List[str]):
        self.extra_packages = [
            os.path.abspath(extra_package) for extra_package in extra_packages
        ]

    @classmethod
    def from_config_path(cls, config_path: str) -> "ADFImplementer":
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
    def from_config_file(cls, config_file: TextIO) -> "ADFImplementer":
        return cls.from_yaml_file(config_file)

    @classmethod
    def from_yaml_file(cls, yaml_file: TextIO):
        return cls.from_config(yaml.load(yaml_file.read(), Loader=yaml.Loader))

    @classmethod
    def from_yaml_string(cls, yaml_string: str):
        return cls.from_config(yaml.safe_load(yaml_string))

    @classmethod
    def get_implementer_class(cls, config: dict) -> Type["ADFImplementer"]:
        if cls._class_key not in config:
            raise ValueError(
                f"Missing essential key '{cls._class_key}' in implementer config."
            )
        implementer_class = getattr(
            import_module(".".join(config[cls._class_key].split(".")[:-1])),
            config[cls._class_key].split(".")[-1],
        )
        if not isclass(implementer_class):
            raise ValueError(
                f"Given implementer class '{config[cls._class_key]}' is not a class, it's a '{type(implementer_class)}'."
            )
        if not issubclass(implementer_class, cls):
            raise ValueError(
                f"Given implementer class '{config[cls._class_key]}' is not an ADF implementer , it's a '{implementer_class}'."
            )
        return implementer_class

    @classmethod
    def from_config(cls, config: dict) -> "ADFImplementer":
        implementer_class = cls.get_implementer_class(config)
        logging.info(
            f"Resolved implementer class '{implementer_class.__name__}' from module '{implementer_class.__module__}'."
        )
        class_config = deepcopy(config)
        del class_config[cls._class_key]
        extra_packages = (
            class_config.pop(cls._pkgs_key) if cls._pkgs_key in class_config else []
        )
        implementer = implementer_class.from_class_config(class_config)
        implementer.add_packages(extra_packages)
        return implementer

    @abstractmethod
    def output_prebuilt_config(self, icp: str) -> Dict:
        pass

    @classmethod
    @abstractmethod
    def from_class_config(cls, config: dict) -> "ADFImplementer":
        pass

    @abstractmethod
    def setup_implementer(self, icp: str):
        pass

    @abstractmethod
    def setup_implementer_flows(self, flows: ADFCollection, icp: str, fcp: str):
        pass

    def install_extra_packages(self) -> None:
        for extra_package in self.extra_packages:
            logging.info(f"Installing package '{extra_package}' locally...")
            run_command("python3 setup.py install", cwd=extra_package)

    @abstractmethod
    def update_code(self) -> None:
        pass

    @abstractmethod
    def destroy(self):
        pass

    @abstractmethod
    def get_log_path(self, step: ADFStep, batch_id: str) -> str:
        pass

    @abstractmethod
    def get_err_path(self, step: ADFStep, batch_id: str) -> str:
        pass

    @classmethod
    def main(cls, sys_args: Optional[List[str]] = None) -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] %(levelname)s : %(message)s",
            datefmt="%d/%m/%Y %H:%M:%S",
            stream=sys.stdout,
        )
        sys_args = sys_args or sys.argv[1:]
        parser = ArgumentParser("Implementer main command", add_help=False)
        parser.add_argument(
            "icp",
            metavar="IMPLEMENTER-CONFIG-PATH",
            type=str,
            help="Implementer config path",
        )
        parser.add_argument(
            "subcommand",
            metavar="SUBCOMMAND",
            type=str,
            help=f"Implementer mode",
        )
        main_args, sub_args = parser.parse_known_args(sys_args)
        implementer = cls.from_config_path(main_args.icp)
        subcommands = implementer.get_subcommands()
        subcommand = subcommands[main_args.subcommand](implementer)
        subcommand(main_args, sub_args)

    @staticmethod
    def get_subcommands() -> Dict[str, Type[Subcommand]]:
        return {
            "setup-implementer": SetupImplementerSubcommand,
            "update-code": UpdateCodeSubcommand,
            "setup-flows": SetupFlowsSubcommand,
            "orchestrate": OrchestrateSubcommand,
            "apply-step": ApplyStepSubCommand,
            "apply-combination-step": ApplyCombinationStepSubCommand,
            "reset-batches": ResetBatchesSubcommand,
            "output-prebuilt": OutputPrebuiltConfigSubcommand,
        }

    def setup_layers(self):
        for layer in self.layers.values():
            logging.info(f"Setting up layer {layer}...")
            layer.setup_layer()

    def destroy_layers(self):
        for layer in self.layers.values():
            logging.info(f"Destroying layer {layer}...")
            layer.destroy()

    def setup_flows(self, flows: ADFCollection, icp: str, fcp: str):
        self.setup_implementer_flows(flows, icp, fcp)
        for layer_name, layer in self.layers.items():
            logging.info(f"SETTING UP LAYER {str(layer)}")
            layer.setup_steps(
                [
                    step
                    for flow in flows
                    for step in flow.steps
                    if step.layer == layer_name and not step.get_output_steps()
                ]
            )
        for flow in flows:
            init_step = flow.steps[0]
            if isinstance(init_step, ADFCombinationStep):
                for input_step in init_step.input_steps:
                    transition = self.get_transition(input_step, init_step)
                    logging.info(
                        f"SETTING UP COMBINATION TRANSITION {str(transition)} :: {str(input_step)}->{str(init_step)}",
                    )
                    transition.setup_read_in(input_step, init_step)
            if isinstance(init_step, ADFReceptionStep):
                for input_step in init_step.input_steps:
                    transition = self.get_transition(input_step, init_step)
                    logging.info(
                        f"SETTING UP RECEPTION TRANSITION {str(transition)} :: {str(input_step)}->{str(init_step)}",
                    )
                    transition.setup_write_out(input_step, init_step)
            for step_in, step_out in zip(flow.steps[:-1], flow.steps[1:]):
                transition = self.get_transition(step_in, step_out)
                if transition is not None:
                    if step_out.get_transition_write_out(transition):
                        logging.info(
                            f"SETTING UP WRITE OUT FLOW TRANSITION {str(transition)} :: {str(step_in)}->{str(step_out)}",
                        )
                        transition.setup_write_out(step_in, step_out)
                    else:
                        logging.info(
                            f"SETTING UP READ IN FLOW TRANSITION {str(transition)} :: {str(step_in)}->{str(step_out)}",
                        )
                        transition.setup_read_in(step_in, step_out)

    def orchestrate(
        self,
        flows: ADFCollection,
        icp: str,
        fcp: str,
        synchronous: bool = False,
        max_loops: Optional[int] = None,
    ):
        logging.info("Validating flows...")
        flows.validate()
        logging.info("Orchestrating flows...")
        n_loops = 0
        while (max_loops is None) or (n_loops < max_loops):
            sys.stdout.flush()
            n_loops += 1
            try:
                n_batches = self.run_flows(flows, icp, fcp, synchronous)
                if synchronous:
                    logging.info(f"Ran {n_batches} batches this run !")
                    if not n_batches:
                        logging.info(
                            "No batches this run, exiting synchronous orchestration.",
                        )
                        break
            except KeyboardInterrupt:
                logging.info(
                    "RECEIVED KEYBOARD INTERRUPT : Final flow run before exit.",
                )
                self.run_flows(flows, icp, fcp, synchronous)
                for (step, batch_id), process in self.subprocesses.items():
                    if process is not None:
                        logging.info(f"SENDING SIGINT TO PID {process.pid}")
                        process.send_signal(SIGINT)
                        process.wait()
                        if (
                            self.state_handler.get_batch_status(step, batch_id)
                            == ADFGlobalConfig.STATUS_SUBMITTED
                        ):
                            logging.info(
                                f"DELETING SUBMITTED BATCH {str(step)} :: {batch_id}",
                            )
                            self.remove_batch(step, batch_id)
                break

    def run_flows(
        self, flows: ADFCollection, icp: str, fcp: str, synchronous: bool = False
    ) -> int:
        n_batches = 0
        for flow in flows:
            flow.steps[0].orchestrate_start(self, icp, fcp, synchronous)
            for step in flow.steps[:-1]:
                n_batches += len(
                    self.submit_step(step, step.get_next_step(), icp, fcp, synchronous)
                )
        return n_batches

    def handle_landing(self, landing_step: ADFLandingStep):
        detected = self.state_handler.get_step_all(landing_step)
        for batch_id in self.layers[landing_step.layer].detect_batches(landing_step):
            if batch_id not in detected:
                logging.info(
                    f"DETECTED BATCH '{batch_id}' in step {str(landing_step)}",
                )
                self.state_handler.set_success(landing_step, batch_id)

    def get_transition(
        self, step_in: ADFStep, step_out: ADFStep
    ) -> Optional[ADLTransition]:
        transition_class = self.transition_matrix.get((step_in.layer, step_out.layer))
        if transition_class is None:
            raise ADLUnhandledTransition(self, step_in, step_out)
        return transition_class(self.layers[step_in.layer], self.layers[step_out.layer])

    def handle_step_result(
        self,
        result: Union[AbstractDataStructure, Dict[str, AbstractDataStructure]],
        step: ADFStep,
        batch_id: str,
        default_write_interface: AbstractDataInterface,
    ) -> None:
        logging.info(f"Handling step result for {str(step)}::{batch_id}")
        now = datetime.datetime.utcnow()
        if isinstance(result, dict):
            output_steps: Dict[str, ADFReceptionStep] = {
                output_step.key: output_step for output_step in step.get_output_steps()
            }
            if result.keys() != output_steps.keys():
                raise ValueError(
                    f"Data structure mismatch, got {result.keys()} in output but expected {output_steps.keys()}."
                )
            for key, ads in result.items():
                self.get_transition(step, output_steps[key]).write_batch_data(
                    output_steps[key].meta.format(ads, batch_id, now),
                    output_steps[key],
                    batch_id,
                )
            self.state_handler.set_success(step, batch_id)
            for output_step in output_steps.values():
                self.state_handler.set_success(output_step, batch_id)
        else:
            output_steps: List[ADFReceptionStep] = step.get_output_steps()
            if output_steps:
                raise ValueError(
                    f"Step '{str(step)}' result was not a dictionary yet the step has outputs: {[str(output_step) for output_step in output_steps]}"
                )
            default_write_interface.write_batch_data(
                step.meta.format(result, batch_id, now), step, batch_id
            )
            self.state_handler.set_success(step, batch_id)
        logging.info(f"Finished handling step result for {str(step)}::{batch_id}")

    def submit_step(
        self,
        step_in: ADFStep,
        step_out: ADFStep,
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ) -> List[str]:
        transition = self.get_transition(step_in, step_out)
        layer = (
            self.layers[step_in.layer]
            if step_out.get_transition_write_out(transition)
            else self.layers[step_out.layer]
        )
        to_process = step_out.sequencer.to_process(
            self.state_handler, step_in, step_out
        )
        for batch_id in to_process:
            if layer.skip_submit():
                logging.info(
                    f"Skipping submission of '{batch_id} :: {str(step_in)} -> {str(step_out)}' in layer '{str(layer)}'.",
                )
                return to_process
            self.state_handler.set_submitted(step_out, batch_id)
            logging.info(
                f"SUBMITTING BATCH {batch_id} :: {str(step_in)} -> {str(step_out)} to layer {str(layer)}",
            )
            try:
                self.subprocesses[(step_out, batch_id)] = layer.submit_step(
                    step_in=step_in,
                    step_out=step_out,
                    batch_id=batch_id,
                    implementer=self,
                    icp=icp,
                    fcp=fcp,
                    synchronous=synchronous,
                )
            except Exception as e:
                self.state_handler.set_failed(
                    step_out,
                    batch_id,
                    msg=f"ON SUBMIT -> {e.__class__.__name__} : {str(e)}",
                )
                if synchronous:
                    logging.info(f"ON SUBMIT -> {e.__class__.__name__} : {str(e)}")
                    raise e
        return to_process

    def apply_step(
        self,
        step_in: ADFStep,
        step_out: ADFStep,
        batch_id: str,
    ) -> None:
        logging.info(f"Applying {str(step_in)} -> {str(step_out)}...")
        self.state_handler.set_running(step_out, batch_id)
        try:
            transition = self.get_transition(step_in, step_out)
            layer_in = self.layers[step_in.layer]
            layer_out = self.layers[step_out.layer]
            if step_out.get_transition_write_out(transition):
                read_interface = layer_in
                write_interface = transition
            else:
                read_interface = transition
                write_interface = layer_out
            ads_args, ads_kwargs = step_out.data_loader.get_ads_args(
                data_interface=read_interface,
                step_in=step_in,
                step_out=step_out,
                batch_id=batch_id,
                state_handler=self.state_handler,
            )
            result = step_out.apply_batch_function(
                ads_args=ads_args, ads_kwargs=ads_kwargs
            )
            self.handle_step_result(result, step_out, batch_id, write_interface)
        except Exception as e:
            self.state_handler.set_failed(
                step_out, batch_id, msg=f"ON APPLY -> {e.__class__.__name__} : {str(e)}"
            )
            raise e
        except KeyboardInterrupt as e:
            self.state_handler.set_failed(
                step_out, batch_id, msg=f"ON APPLY -> {e.__class__.__name__} : {str(e)}"
            )
            raise e

    def submit_combination_step(
        self,
        combination_step: ADFCombinationStep,
        icp: str,
        fcp: str,
        synchronous: bool = False,
    ):
        layer = self.layers[combination_step.layer]
        for batch_args, batch_id in combination_step.sequencer.to_process(
            self.state_handler, combination_step
        ):
            if layer.skip_submit():
                logging.info(
                    f"Skipping submission of '{combination_step}' :: {batch_id} in layer '{str(layer)}'.",
                )
                return
            self.state_handler.set_submitted(combination_step, batch_id)
            logging.info(
                f"SUBMITTING COMBINATION BATCH {batch_id} :: {combination_step}",
            )
            try:
                self.subprocesses[
                    (combination_step, batch_id)
                ] = layer.submit_combination_step(
                    combination_step=combination_step,
                    batch_args=batch_args,
                    batch_id=batch_id,
                    implementer=self,
                    icp=icp,
                    fcp=fcp,
                    synchronous=synchronous,
                )
            except Exception as e:
                self.state_handler.set_failed(
                    combination_step,
                    batch_id,
                    msg=f"ON SUBMIT -> {e.__class__.__name__} : {str(e)}",
                )
                if synchronous:
                    raise e

    def apply_combination_step(
        self,
        combination_step: ADFCombinationStep,
        batch_args: List[str],
        batch_id: str,
    ):
        logging.info(f"Applying combination step {str(combination_step)}...")
        self.state_handler.set_running(combination_step, batch_id)
        try:
            layer = self.layers[combination_step.layer]
            if len(batch_args) != len(combination_step.input_steps):
                raise CombinationStepError(
                    step=combination_step,
                    context=f"BATCH:{batch_id}",
                    msg=f"Mismatched arg length (config : {len(combination_step.input_steps)}, provided : {len(batch_args)}).",
                )
            ads_args, ads_kwargs = combination_step.data_loader.get_ads_args(
                data_interface=layer,
                step_in=combination_step,
                step_out=combination_step,
                batch_id=batch_id,
                state_handler=self.state_handler,
            )
            for data_loader, input_step, input_batch_id in zip(
                combination_step.data_loaders, combination_step.input_steps, batch_args
            ):
                transition = self.get_transition(input_step, combination_step)
                input_ads_args, input_ads_kwargs = data_loader.get_ads_args(
                    data_interface=transition,
                    step_in=input_step,
                    step_out=combination_step,
                    batch_id=input_batch_id,
                    state_handler=self.state_handler,
                )
                ads_args += input_ads_args
                ads_kwargs.update(input_ads_kwargs)
            result = combination_step.apply_batch_function(ads_args, ads_kwargs)
            self.handle_step_result(result, combination_step, batch_id, layer)
        except Exception as e:
            self.state_handler.set_failed(
                combination_step,
                batch_id,
                msg=f"ON APPLY -> {e.__class__.__name__} : {str(e)}",
            )
            raise e
        except KeyboardInterrupt as e:
            self.state_handler.set_failed(
                combination_step,
                batch_id,
                msg=f"ON APPLY -> {e.__class__.__name__} : {str(e)}",
            )
            raise e

    def get_step_removal_interface(self, step: ADFStep) -> AbstractDataInterface:
        try:
            # TODO : not robust in case of differing transitions
            # TODO : need additional validation for receptions steps to find a universal solution
            # TODO : either all the same transition step or read in transition
            return self.get_transition(step.get_upstream_steps()[0], step)
        except IndexError:
            return self.layers[step.layer]

    def remove_batch(self, step: ADFStep, batch_id: str) -> None:
        logging.info(f"Removing batch '{str(step)}::{batch_id}'...")
        self.state_handler.set_deleting(step, batch_id)
        if not step.get_output_steps():
            step_removal_interface = self.get_step_removal_interface(step)
            logging.info(
                f"Deleting batch '{str(step)}::{batch_id}' using interface {str(step_removal_interface)}"
            )
            step_removal_interface.delete_batch(step, batch_id)
        self.state_handler.delete_entry(step, batch_id)

    def remove_downstream(
        self, step: ADFStep, batch_ids: List[str]
    ) -> List[Tuple[ADFStep, List[str]]]:
        if isinstance(step, ADFLandingStep):
            raise RuntimeError(
                f"ERROR : Activated reset protection on landing step {str(step)}."
            )
        logging.info(f"Resetting downstream for '{str(step)}::{batch_ids}'...")
        downstream = self.state_handler.get_downstream(step, batch_ids)
        for downstream_step, downstream_batch_ids in downstream[::-1]:
            for downstream_batch_id in downstream_batch_ids:
                self.remove_batch(downstream_step, downstream_batch_id)
        return downstream

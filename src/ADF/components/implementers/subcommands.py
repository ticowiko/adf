import os
import yaml
import logging

from argparse import ArgumentParser, Namespace
from abc import ABC, abstractmethod
from typing import List

import ADF
from ADF.components.flow_config import ADFCombinationStep, ADFCollection
from ADF.exceptions import CombinationStepError
from ADF.config import ADFGlobalConfig


class Subcommand(ABC):
    def __init__(self, implementer: "ADF.components.implementers.ADFImplementer"):
        self.implementer = implementer
        self.parser = self.get_parser()

    @abstractmethod
    def get_parser(self) -> ArgumentParser:
        pass

    @abstractmethod
    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        pass

    def __call__(self, main_args: Namespace, sys_sub_args: List[str]) -> None:
        return self.handle(main_args, self.parser.parse_args(sys_sub_args))


class SetupImplementerSubcommand(Subcommand):
    def get_parser(self):
        parser = ArgumentParser("Implementer setup sub-command")
        parser.add_argument(
            "-d",
            "--destroy",
            action="store_true",
            help="Set flag to destroy all resources, data, and processing state",
        )
        return parser

    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        if sub_args.destroy:
            logging.info("Destroying layers...")
            self.implementer.destroy_layers()
            logging.info("Destroying state handler...")
            self.implementer.state_handler.destroy()
            logging.info("Destroying implementer...")
            self.implementer.destroy()
        else:
            logging.info("Setting up implementer...")
            self.implementer.setup_implementer(main_args.icp)
            logging.info("Setting up state handler...")
            self.implementer.state_handler.setup()
            logging.info("Setting up layers...")
            self.implementer.setup_layers()


class UpdateCodeSubcommand(Subcommand):
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser("Update code sub-command")
        return parser

    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        self.implementer.update_code()


class SetupFlowsSubcommand(Subcommand):
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser("Flow setup sub-command")
        parser.add_argument(
            "fcp",
            metavar="FLOW-COLLECTION-CONFIG-PATH",
            type=str,
            help="Flow collection config path",
        )
        return parser

    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        self.implementer.setup_flows(
            ADFCollection.from_config_path(sub_args.fcp), main_args.icp, sub_args.fcp
        )


class ApplyStepSubCommand(Subcommand):
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser("Apply step sub-command")
        parser.add_argument(
            "fcp",
            metavar="FLOW-COLLECTION-CONFIG-PATH",
            type=str,
            help="Flow collection config path",
        )
        parser.add_argument(
            "flow_name_in",
            metavar="FLOW-NAME-IN",
            type=str,
            help="Flow name for step in",
        )
        parser.add_argument(
            "step_name_in",
            metavar="STEP-NAME-IN",
            type=str,
            help="Step name for step in",
        )
        parser.add_argument(
            "flow_name_out",
            metavar="FLOW-NAME-OUT",
            type=str,
            help="Flow name for step out",
        )
        parser.add_argument(
            "step_name_out",
            metavar="STEP-NAME-OUT",
            type=str,
            help="Step name for step out",
        )
        parser.add_argument(
            "batch_id", metavar="BATCH-ID", type=str, help="Batch ID for apply"
        )
        return parser

    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        flows = ADFCollection.from_config_path(sub_args.fcp)
        step_in = flows.get_step(sub_args.flow_name_in, sub_args.step_name_in)
        step_out = flows.get_step(sub_args.flow_name_out, sub_args.step_name_out)
        self.implementer.apply_step(step_in, step_out, sub_args.batch_id)


class ApplyCombinationStepSubCommand(Subcommand):
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser("Apply combination step sub-command")
        parser.add_argument(
            "fcp",
            metavar="FLOW-COLLECTION-CONFIG-PATH",
            type=str,
            help="Flow collection config path",
        )
        parser.add_argument(
            "flow_name",
            metavar="FLOW-NAME",
            type=str,
            help="Flow name",
        )
        parser.add_argument(
            "step_name",
            metavar="STEP-NAME",
            type=str,
            help="Step name",
        )
        parser.add_argument(
            "batch_id", metavar="BATCH-ID", type=str, help="Batch ID for apply"
        )
        parser.add_argument(
            "batch_args",
            metavar="BATCH-ARGS",
            type=str,
            help="Input batch IDs for apply",
            nargs="+",
        )
        return parser

    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        flows = ADFCollection.from_config_path(sub_args.fcp)
        combination_step = flows.get_step(sub_args.flow_name, sub_args.step_name)
        if not isinstance(combination_step, ADFCombinationStep):
            raise CombinationStepError(
                combination_step,
                f"BATCH:{sub_args.batch_id}",
                "Not actually a combination step.",
            )
        self.implementer.apply_combination_step(
            combination_step=combination_step,
            batch_args=sub_args.batch_args,
            batch_id=sub_args.batch_id,
        )


class OrchestrateSubcommand(Subcommand):
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser("Orchestration sub-command")
        parser.add_argument(
            "fcp",
            metavar="FLOW-COLLECTION-CONFIG-PATH",
            type=str,
            help="Flow collection config path",
        )
        parser.add_argument(
            "-s",
            "--synchronous",
            action="store_true",
            help="run synchronously (for supported layers)",
        )
        parser.add_argument(
            "-l",
            "--loops",
            type=int,
            help="Number of orchestration loops to run before exit",
            default=None,
        )
        return parser

    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        self.implementer.orchestrate(
            ADFCollection.from_config_path(sub_args.fcp),
            os.path.abspath(main_args.icp),
            os.path.abspath(sub_args.fcp),
            sub_args.synchronous,
            sub_args.loops,
        )


class ResetBatchesSubcommand(Subcommand):
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser("Batch reset subcommand")
        parser.add_argument(
            "fcp",
            metavar="FLOW-COLLECTION-CONFIG-PATH",
            type=str,
            help="Flow collection config path",
        )
        parser.add_argument(
            "flow_name",
            metavar="FLOW-NAME",
            type=str,
            help="Flow name",
        )
        parser.add_argument(
            "step_name",
            metavar="STEP-NAME",
            type=str,
            help="Step name",
        )
        parser.add_argument(
            "-s",
            "--status",
            metavar="STATUS",
            type=str,
            help=f"Limit reset to this status, can be one of {', '.join(ADFGlobalConfig.STATUSES)}",
            default=None,
            choices=ADFGlobalConfig.STATUSES,
        )
        parser.add_argument(
            "-b",
            "--batch-id",
            metavar="BATCH-ID",
            type=str,
            help="Limit reset to this batch id",
            default=None,
        )
        parser.add_argument(
            "-d",
            "--downstream",
            action="store_true",
            help="display downstream without resetting",
        )
        return parser

    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        flows = ADFCollection.from_config_path(sub_args.fcp)
        step = flows.get_step(sub_args.flow_name, sub_args.step_name)
        if sub_args.batch_id:
            reset = (sub_args.status is None) or (
                self.implementer.state_handler.get_batch_status(step, sub_args.batch_id)
                == sub_args.status
            )
            if reset:
                batch_ids = [sub_args.batch_id]
            else:
                logging.info(
                    f"No batch {str(step)}::{sub_args.batch_id} found in state {sub_args.status}, skipping reset"
                )
                batch_ids = []
        else:
            batch_ids = (
                self.implementer.state_handler.get_step_per_status(
                    step, sub_args.status
                )
                if sub_args.status
                else self.implementer.state_handler.get_step_all(step)
            )
        if sub_args.downstream:
            print("LISTING DOWNSTREAM :")
            for (
                downstream_step,
                downstream_batch_ids,
            ) in self.implementer.state_handler.get_downstream(step, batch_ids):
                print(f"{str(downstream_step)}::{downstream_batch_ids}")
        else:
            self.implementer.remove_downstream(step, batch_ids)


class OutputPrebuiltConfigSubcommand(Subcommand):
    def get_parser(self) -> ArgumentParser:
        parser = ArgumentParser("Flow setup sub-command")
        parser.add_argument(
            "pbicp",
            metavar="PREBUILT-CONFIG-OUTPUT-PATH",
            type=str,
            help="Output path for prebuilt config",
        )
        return parser

    def handle(self, main_args: Namespace, sub_args: Namespace) -> None:
        yaml.dump(
            self.implementer.output_prebuilt_config(main_args.icp),
            open(sub_args.pbicp, "w"),
            Dumper=yaml.Dumper,
        )

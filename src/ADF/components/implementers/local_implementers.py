import os
import yaml
import shutil
import logging

from abc import ABC
from typing import Dict, List, Optional, Tuple, Type
from pathlib import Path
from importlib import import_module

from ADF.components.layers import AbstractDataLayer
from ADF.components.flow_config import ADFStep, ADFCollection
from ADF.components.implementers import ADFImplementer
from ADF.components.state_handlers import AbstractStateHandler, SQLStateHandler
from ADF.components.layer_transitions import (
    ADLTransition,
    TransitionLocalToLocal,
    UniversalTransitionToSQL,
    SameHostSQLToSQL,
)
from ADF.components.layers import (
    LocalListOfDictsFileDataLayer,
    LocalPandasFileDataLayer,
    LocalSparkFileDataLayer,
    SQLiteDataLayer,
    PostgreSQLDataLayer,
)
from ADF.utils import setup_postgres_db, run_command


class LocalImplementer(ADFImplementer, ABC):
    def __init__(
        self,
        root_path: str,
        layers: Dict[str, AbstractDataLayer],
        state_handler: AbstractStateHandler,
        transitions: Optional[List[Type[ADLTransition]]] = None,
        transition_matrix: Optional[Dict[Tuple[str, str], Type[ADLTransition]]] = None,
    ):
        super().__init__(
            layers=layers,
            state_handler=state_handler,
            transitions=transitions,
            transition_matrix=transition_matrix,
        )
        self.root_path = root_path

    def setup_implementer(self, icp: str):
        Path(os.path.join(self.root_path, "logs")).mkdir(parents=True, exist_ok=True)
        self.update_code()

    def setup_implementer_flows(self, flows: ADFCollection, icp: str, fcp: str):
        pass

    def update_code(self) -> None:
        self.install_extra_packages()

    def destroy(self):
        if os.path.isdir(self.root_path):
            shutil.rmtree(self.root_path)

    def get_log_path(self, step: ADFStep, batch_id: str) -> str:
        return os.path.join(self.root_path, "logs", f"{str(step)}.{batch_id}.log")

    def get_err_path(self, step: ADFStep, batch_id: str) -> str:
        return os.path.join(self.root_path, "logs", f"{str(step)}.{batch_id}.err")

    def output_prebuilt_config(self, icp: str) -> Dict:
        return yaml.load(open(icp, "r").read(), Loader=yaml.Loader)


class MonoLayerLocalImplementer(LocalImplementer):
    @classmethod
    def from_class_config(cls, config: dict) -> "MonoLayerLocalImplementer":
        root_path = config["root_path"]
        state_handler = SQLStateHandler(
            f"sqlite:///{os.path.join(root_path, 'state.db')}?check_same_thread=false",
            echo=False,
        )
        local_layer_class = getattr(
            import_module(config["layer"]["module"]), config["layer"]["class"]
        )
        layers = {
            layer_name: local_layer_class(
                as_layer=layer_name,
                root_path=os.path.join(config["root_path"], layer_name),
            )
            for layer_name in config["layers"]
        }
        return cls(
            root_path=root_path,
            layers=layers,
            state_handler=state_handler,
            transitions=[TransitionLocalToLocal],
        )


class MultiLayerLocalImplementer(LocalImplementer):
    @classmethod
    def from_class_config(cls, config: dict) -> "ADFImplementer":
        postgres_config = config.get("postgres_config")
        if postgres_config:
            if "admin_user" in postgres_config and "admin_pw" in postgres_config:
                logging.info(
                    "Setting up DB and technical user using postgres admin credentials."
                )
                setup_postgres_db(
                    host=postgres_config.get("host", "localhost"),
                    port=postgres_config.get("port", 5432),
                    db=postgres_config["db"],
                    admin_user=postgres_config["admin_user"],
                    admin_pw=postgres_config["admin_pw"],
                    users=[
                        {"user": postgres_config["user"], "pw": postgres_config["pw"]}
                    ],
                )
            else:
                logging.info(
                    "Skipping DB and user setup as no postgres admin credentials were given."
                )
        elif config["layers"].get("postgres", []):
            raise RuntimeError(
                "Cannot use postgres layers without passing a postgres config."
            )
        root_path = config["root_path"]
        return cls(
            root_path=root_path,
            layers={
                **{
                    layer: LocalListOfDictsFileDataLayer(
                        as_layer=layer, root_path=os.path.join(root_path, layer)
                    )
                    for layer in config["layers"].get("list_of_dicts", [])
                },
                **{
                    layer: LocalPandasFileDataLayer(
                        as_layer=layer, root_path=os.path.join(root_path, layer)
                    )
                    for layer in config["layers"].get("pandas", [])
                },
                **{
                    layer: LocalSparkFileDataLayer(
                        as_layer=layer, root_path=os.path.join(root_path, layer)
                    )
                    for layer in config["layers"].get("spark", [])
                },
                **{
                    layer: SQLiteDataLayer(
                        as_layer=layer,
                        db_path=os.path.join(root_path, f"{layer}.db"),
                        table_prefix=f"{layer}_",
                    )
                    for layer in config["layers"].get("sqlite", [])
                },
                **{
                    layer: PostgreSQLDataLayer(
                        as_layer=layer,
                        engine="postgresql",
                        host=postgres_config.get("host", "localhost"),
                        port=postgres_config.get("port", 5432),
                        db=postgres_config["db"],
                        user=postgres_config["user"],
                        pw=postgres_config["pw"],
                        table_prefix=f"{layer}_",
                    )
                    for layer in config["layers"].get("postgres", [])
                },
            },
            state_handler=SQLStateHandler(
                f"sqlite:///{os.path.join(root_path, 'state.db')}?check_same_thread=false",
                echo=False,
            ),
            transitions=[
                TransitionLocalToLocal,
                SameHostSQLToSQL,
                UniversalTransitionToSQL,
            ],
        )

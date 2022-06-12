import os
import csv
import json
import glob
import shutil
import logging

from pyspark.sql import SparkSession

from typing import List, Any, Tuple, Dict, Type
from pathlib import Path
from abc import ABC, abstractmethod

from ADF.components.data_structures import (
    AbstractDataStructure,
    PandasDataStructure,
    ListOfDictsDataStructure,
    SparkDataStructure,
)
from ADF.components.flow_config import ADFStep
from ADF.components.layers import AbstractDataLayer
from ADF.utils import MetaCSVToPandas, PandasToMetaCSV


class ADFJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, complex):
            return str(o)
        return super().default(o)


class LocalFileDataLayer(AbstractDataLayer, ABC):
    def __init__(self, as_layer: str, root_path: str):
        super().__init__(as_layer)
        self.root_path = root_path

    @staticmethod
    @abstractmethod
    def get_ads_class() -> Type[AbstractDataStructure]:
        pass

    def setup_layer(self) -> None:
        Path(self.root_path).mkdir(parents=True, exist_ok=True)

    def destroy(self) -> None:
        if os.path.isdir(self.root_path):
            shutil.rmtree(self.root_path)

    def _step_path(self, step: ADFStep) -> str:
        return os.path.join(
            self.root_path,
            step.flow.collection.name,
            step.flow.name,
            step.name,
            step.version,
        )

    def _ads_path(self, step: ADFStep, batch_id: str) -> str:
        return os.path.join(self._step_path(step), batch_id) + ".csv"

    @classmethod
    def _batch_id_from_path(cls, path: str) -> str:
        return ".".join(os.path.basename(path).split(".")[:-1])

    def setup_steps(self, steps: List[ADFStep]) -> None:
        for step in steps:
            Path(self._step_path(step)).mkdir(parents=True, exist_ok=True)

    def read_batch_data(self, step: ADFStep, batch_id: str) -> AbstractDataStructure:
        reader = csv.reader(open(self._ads_path(step, batch_id), "r"))
        header = next(reader)
        col_map, meta = MetaCSVToPandas.cols_to_meta(header)
        data = [
            {col_map[key]: val for key, val in row.items()}
            for row in csv.DictReader(
                open(self._ads_path(step, batch_id), "r"), skipinitialspace=True
            )
        ]
        return self.read_meta_data(meta=meta, data=data)

    def read_full_data(self, step: ADFStep) -> AbstractDataStructure:
        ads_all = [
            self.read_batch_data(step, batch_id)
            for batch_id in self.detect_batches(step)
        ]
        return ads_all[0].union(*ads_all[1:])

    def read_meta_data(self, meta: Dict, data: List) -> AbstractDataStructure:
        return self.get_ads_class().from_list_of_dicts(data=data, meta=meta)

    def write_batch_data(
        self, ads: AbstractDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        meta, data = self.dump_ads(ads)
        self.write_meta_data(step, batch_id, meta, data)

    def write_meta_data(
        self, step: ADFStep, batch_id: str, meta: Dict, data: List
    ) -> None:
        col_map = PandasToMetaCSV.meta_to_cols(
            data[0].keys() if data else meta.keys(), meta
        )
        renamed_data = []
        for row in data:
            renamed_data.append({renamed: row[col] for col, renamed in col_map.items()})
        dict_writer = csv.DictWriter(
            open(self._ads_path(step, batch_id), "w"), list(col_map.values())
        )
        dict_writer.writeheader()
        dict_writer.writerows(renamed_data)

    @abstractmethod
    def dump_ads(self, ads: AbstractDataStructure) -> Tuple[Dict, List]:
        pass

    def detect_batches(
        self,
        step: ADFStep,
    ) -> List[str]:
        return [
            self._batch_id_from_path(path)
            for path in glob.glob(os.path.join(self._step_path(step), "*.csv"))
        ]

    def delete_step(self, step: ADFStep) -> None:
        for path in glob.glob(os.path.join(self._step_path(step), "*.csv")):
            os.remove(path)

    def delete_batch(self, step: ADFStep, batch_id: str) -> None:
        try:
            os.remove(self._ads_path(step, batch_id))
        except FileNotFoundError:
            logging.warning(
                f"WARNING : no file found while deleting batch '{str(step)}::{batch_id}' in layer '{str(self)}'."
            )


class LocalPandasFileDataLayer(LocalFileDataLayer):
    @staticmethod
    def get_ads_class() -> Type[AbstractDataStructure]:
        return PandasDataStructure

    def dump_ads(self, ads: PandasDataStructure) -> Tuple[Dict, List]:
        return (
            {key: str(val) for key, val in ads.df.dtypes.to_dict().items()},
            ads.df.to_dict("records"),
        )


class LocalListOfDictsFileDataLayer(LocalFileDataLayer):
    dump_meta_dict = {
        val: key for key, val in ListOfDictsDataStructure.meta_dict.items()
    }

    @staticmethod
    def get_ads_class() -> Type[AbstractDataStructure]:
        return ListOfDictsDataStructure

    def dump_ads(self, ads: ListOfDictsDataStructure) -> Tuple[Dict, List]:
        return (
            {key: self.dump_meta_dict[val] for key, val in ads.meta.items()},
            ads.data,
        )


class LocalSparkFileDataLayer(LocalFileDataLayer):
    def __init__(self, as_layer: str, root_path: str):
        super().__init__(as_layer, root_path)
        self._spark = None

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = (
                SparkSession.builder.master("local")
                .appName("local-spark-layer")
                .getOrCreate()
            )
            self._spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return self._spark

    @staticmethod
    def get_ads_class() -> Type[AbstractDataStructure]:
        return SparkDataStructure

    def dump_ads(self, ads: SparkDataStructure) -> Tuple[Dict, List]:
        return ads.spark_schema(), ads.df.rdd.map(lambda row: row.asDict()).collect()

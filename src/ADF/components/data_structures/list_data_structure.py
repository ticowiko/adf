import datetime

from copy import deepcopy
from functools import cmp_to_key

import pandas as pd

from pyspark.sql import DataFrame

from typing import Union, List, Tuple, Dict, Callable, Any, Type, Optional
from typing_extensions import Literal

from ADF.components.columns import ListColumn
from ADF.components.data_structures import (
    AbstractDataStructure,
    ADSConcretizer,
    ADSConcretizerDict,
    SparkDataStructure,
)


def compare_func_factory(columns: List[str], asc: List[bool]):
    def compare_rows(row0: Dict[str, Any], row1: Dict[str, Any]):
        for col, is_asc in zip(columns, asc):
            if row0[col] > row1[col]:
                return 1 if is_asc else -1
            elif row0[col] < row1[col]:
                return -1 if is_asc else 1
        return 0

    return compare_rows


class ListOfDictsDataStructure(AbstractDataStructure):
    concretizations = ADSConcretizerDict(
        {
            list: ADSConcretizer(
                concretize=lambda ads: deepcopy(ads.data),
                extract_abstraction_params=lambda ads: {},
                abstract=lambda l, params: ListOfDictsDataStructure(l),
            ),
            dict: ADSConcretizer(
                concretize=lambda ads: {
                    col: [row[col] for row in ads.data] for col in ads.meta
                },
                extract_abstraction_params=lambda ads: {},
                abstract=lambda d, params: ListOfDictsDataStructure(
                    [
                        {col: d[col][i] for col in d}
                        for i in range(len(list(d.values())[0]))
                    ]
                ),
            ),
            pd.DataFrame: ADSConcretizer(
                concretize=lambda ads: pd.DataFrame.from_records(ads.data),
                extract_abstraction_params=lambda ads: {},
                abstract=lambda df, params: ListOfDictsDataStructure(
                    df.to_dict("records")
                ),
            ),
            DataFrame: ADSConcretizer(
                concretize=lambda ads: SparkDataStructure.from_list_of_dicts(
                    ads.data,
                    {
                        col: SparkDataStructure.inverted_type_dict.get(cast, None)
                        for col, cast in ads.meta.items()
                    },
                ).df,
                extract_abstraction_params=lambda ads: {},
                abstract=lambda df, params: ListOfDictsDataStructure(
                    df.rdd.map(lambda row: row.asDict()).collect()
                ),
            ),
        }
    )
    meta_dict = {
        "str": str,
        "int": int,
        "float": float,
        "complex": complex,
        "datetime": datetime.datetime,
        "date": datetime.date,
        "timedelta": datetime.timedelta,
        None: None,
    }

    def __init__(
        self,
        data: List[Dict[str, Any]],
        meta: Optional[Dict[str, Optional[Type]]] = None,
    ):
        super().__init__()
        if not len(data) and (meta is None or not len(meta)):
            raise ValueError(
                f"{self.__class__.__name__} cannot be instantiated with empty list and empty meta."
            )
        self.data = deepcopy(data)
        self.meta = deepcopy(meta) or {}
        self.sync_meta()

    def sync_meta(self):
        for row in self.data:
            for col in row:
                if col not in self.meta:
                    self.meta[col] = None
            for col in self.meta:
                if col not in row:
                    row[col] = None
        for col, cast in self.meta.items():
            if cast is not None:
                for row, casted in zip(self.data, self[col].as_type(cast).data):
                    row[col] = casted

    def list_columns(self) -> List[str]:
        return list(self.meta.keys())

    def get_column(self, col_name: str) -> ListColumn:
        return ListColumn(
            [row[col_name] for row in self.data], meta=self.meta.get(col_name)
        )

    def get_columns(self, cols: List[str]) -> "ListOfDictsDataStructure":
        return ListOfDictsDataStructure(
            data=[
                {key: val for key, val in row.items() if key in cols}
                for row in self.data
            ],
            meta={key: val for key, val in self.meta.items() if key in cols},
        )

    def create_column_from_column(self, col_name: str, col_values: ListColumn) -> None:
        if len(col_values.data) != len(self.data):
            raise ValueError(
                f"{self.__class__.__name__} cannot create column {col_name} of length {len(col_values.data)} (required length : {len(self.data)})."
            )
        for row, value in zip(self.data, col_values.data):
            row[col_name] = value
        self.meta[col_name] = col_values.meta or self.meta.get(col_name)
        self.sync_meta()

    def create_column_from_value(self, col_name: str, value) -> None:
        for row in self.data:
            row[col_name] = value
        self.sync_meta()

    def set_column_from_column(self, col_name: str, col_values: ListColumn) -> None:
        if len(col_values.data) != len(self.data):
            raise ValueError(
                f"{self.__class__.__name__} cannot set column {col_name} of length {len(col_values.data)} (required length : {len(self.data)})."
            )
        for row, value in zip(self.data, col_values.data):
            row[col_name] = value
        self.meta[col_name] = col_values.meta or self.meta.get(col_name)
        self.sync_meta()

    def set_column_from_value(self, col_name: str, value) -> None:
        for row in self.data:
            row[col_name] = value
        self.sync_meta()

    def filter_from_column(self, col: ListColumn) -> "ListOfDictsDataStructure":
        if len(col.data) != len(self.data):
            raise ValueError(
                f"{self.__class__.__name__} cannot filter from column of length {len(col.data)} (required length : {len(self.data)})."
            )
        return ListOfDictsDataStructure(
            data=[row for row, value in zip(self.data, col.data) if value],
            meta=self.meta,
        )

    def set_subset_from_column(
        self,
        col_name: str,
        col_filter: ListColumn,
        col_values: ListColumn,
    ):
        if len(col_filter.data) != len(self.data):
            raise ValueError(
                f"{self.__class__.__name__} cannot set subset of column {col_name} from filter of length {len(col_filter.data)} (required length : {len(self.data)})."
            )
        filter_length = sum([bool(e) for e in col_filter.data])
        if len(col_values.data) != filter_length:
            raise ValueError(
                f"{self.__class__.__name__} cannot set subset of column {col_name} from values of length {len(col_values.data)} (required length : {filter_length})."
            )
        val_index = 0
        for row, filter_val in zip(self.data, col_filter.data):
            if filter_val:
                row[col_name] = col_values.data[val_index]
                val_index += 1
        self.meta[col_name] = col_values.meta or self.meta.get(col_name)
        self.sync_meta()

    def set_subset_from_value(self, col_name: str, col_filter: ListColumn, value: Any):
        if len(col_filter.data) != len(self.data):
            raise ValueError(
                f"{self.__class__.__name__} cannot set subset of column {col_name} from filter of length {len(col_filter.data)} (required length : {len(self.data)})."
            )
        for row, filter_val in zip(self.data, col_filter.data):
            if filter_val:
                row[col_name] = value
        self.sync_meta()

    def rename(self, names: Dict[str, str]) -> "ListOfDictsDataStructure":
        data = []
        for row in self.data:
            data.append({names.get(key, key): val for key, val in row.items()})
        return ListOfDictsDataStructure(
            data=data, meta={names.get(key, key): val for key, val in self.meta.items()}
        )

    def to_list_of_dicts(self) -> List[Dict]:
        return deepcopy(self.data)

    @classmethod
    def from_list_of_dicts(
        cls, data: List[Dict[str, Any]], meta: Optional[Dict[str, str]]
    ) -> "ListOfDictsDataStructure":
        meta = meta or {}
        return ListOfDictsDataStructure(
            data=data,
            meta={key: cls.meta_dict[val] for key, val in meta.items()},
        )

    def count(self) -> int:
        return len(self.data)

    def _join(
        self,
        other: "ListOfDictsDataStructure",
        left_on: List[str],
        right_on: List[str],
        how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
        l_map: Dict[str, str] = None,
        r_map: Dict[str, str] = None,
    ) -> "ListOfDictsDataStructure":
        cross = [
            (
                {l_map.get(col, col): val for col, val in self_row.items()},
                {r_map.get(col, col): val for col, val in other_row.items()},
                i,
                j,
            )
            for i, self_row in enumerate(self.data)
            for j, other_row in enumerate(other.data)
        ]
        if how == "cross":
            return ListOfDictsDataStructure(
                [
                    {
                        **row[0],
                        **row[1],
                    }
                    for row in cross
                ]
            )
        else:
            join = [
                row
                for row in cross
                if [row[0][l_map.get(col, col)] for col in left_on]
                == [row[1][r_map.get(col, col)] for col in right_on]
            ]
            if how in ["left", "outer"]:
                for i, self_row in enumerate(self.data):
                    if i not in [e[2] for e in join]:
                        join.append(
                            (
                                {
                                    l_map.get(col, col): val
                                    for col, val in self_row.items()
                                },
                                {
                                    r_map.get(col, col): None
                                    for col in other.list_columns()
                                    if col not in right_on
                                },
                                i,
                                -1,
                            )
                        )
            if how in ["right", "outer"]:
                for j, other_row in enumerate(other.data):
                    if j not in [e[3] for e in join]:
                        join.append(
                            (
                                {
                                    l_map.get(col, col): None
                                    for col in self.list_columns()
                                },
                                {
                                    r_map.get(col, col): val
                                    for col, val in other_row.items()
                                    if col not in left_on
                                },
                                j,
                                -1,
                            )
                        )
            return ListOfDictsDataStructure(
                [
                    {
                        **row[0],
                        **row[1],
                    }
                    for row in join
                ]
            )

    def _group_by(
        self,
        keys: List[str],
        outputs: Dict[
            str,
            Tuple[
                Callable[[AbstractDataStructure], Any],
                Union[
                    Type[str],
                    Type[int],
                    Type[float],
                    Type[complex],
                    Type[datetime.datetime],
                    Type[datetime.date],
                    Type[datetime.timedelta],
                ],
            ],
        ],
    ) -> "ListOfDictsDataStructure":
        data = []
        groups = self.distinct(keys)
        for group in groups.data:
            subset = self
            for key in keys:
                subset = subset[subset[key] == group[key]]
            data.append(
                {
                    **group,
                    **{
                        output: cast(agg(subset))
                        for output, (agg, cast) in outputs.items()
                    },
                }
            )
        return ListOfDictsDataStructure(data)

    def _union(
        self, *others: "ListOfDictsDataStructure", all: bool
    ) -> "ListOfDictsDataStructure":
        data = self.data
        for other in others:
            data += other.data
        if all:
            return ListOfDictsDataStructure(data)
        else:
            return ListOfDictsDataStructure(data).distinct()

    def _distinct(self, keys: Optional[List[str]] = None) -> "ListOfDictsDataStructure":
        keys = keys or self.list_columns()
        distinct = []
        for row in self.data:
            seen = False
            for comp in distinct:
                for key in keys:
                    if all([row[key] == comp[key]]):
                        seen = True
            if not seen:
                distinct.append({col: val for col, val in row.items() if col in keys})
        return ListOfDictsDataStructure(distinct)

    def _apply(
        self, output_column: str, func: Callable[[Dict], Any], cast: Type
    ) -> "ListOfDictsDataStructure":
        return ListOfDictsDataStructure(
            meta={**self.meta, output_column: cast},
            data=[
                {
                    **row,
                    output_column: cast(func(row)),
                }
                for row in self.data
            ],
        )

    def _sort(self, cols: List[str], asc: List[bool]) -> "ListOfDictsDataStructure":
        data = deepcopy(self.data)
        meta = deepcopy(self.meta)
        return ListOfDictsDataStructure(
            data=sorted(data, key=cmp_to_key(compare_func_factory(cols, asc))),
            meta=meta,
        )

    def _limit(self, n: int) -> "ListOfDictsDataStructure":
        return ListOfDictsDataStructure(
            data=[row for row in deepcopy(self.data)[:n]],
            meta=deepcopy(self.meta),
        )

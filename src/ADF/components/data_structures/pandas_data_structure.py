import datetime

from typing import Union, List, Tuple, Dict, Callable, Any, Type, Optional
from typing_extensions import Literal

from pandas import DataFrame, concat

import pyspark.sql as ps

from ADF.components.columns import PandasColumn
from ADF.components.data_structures import (
    AbstractDataStructure,
    ADSConcretizer,
    ADSConcretizerDict,
)


class PandasDataStructure(AbstractDataStructure):
    concretizations = ADSConcretizerDict(
        {
            DataFrame: ADSConcretizer(
                concretize=lambda ads: ads.df,
                extract_abstraction_params=lambda ads: {},
                abstract=lambda df, params: PandasDataStructure(df),
            ),
            ps.DataFrame: ADSConcretizer(
                concretize=lambda ads: ps.SparkSession.builder.master("local")
                .appName("spark-concretizer")
                .getOrCreate()
                .createDataFrame(
                    ads.df,
                    schema=", ".join([f"{col} string" for col in ads.df.columns]),
                ),
                extract_abstraction_params=lambda ads: {},
                abstract=lambda df, params: PandasDataStructure(df.toPandas()),
            ),
            list: ADSConcretizer(
                concretize=lambda ads: ads.df.to_dict("records"),
                extract_abstraction_params=lambda ads: {},
                abstract=lambda l, params: PandasDataStructure(
                    DataFrame.from_records(l)
                ),
            ),
            dict: ADSConcretizer(
                concretize=lambda ads: ads.df.to_dict(),
                extract_abstraction_params=lambda ads: {},
                abstract=lambda d, params: PandasDataStructure(DataFrame.from_dict(d)),
            ),
        }
    )

    def __init__(self, df: DataFrame):
        super().__init__()
        self.df = df

    def list_columns(self) -> List[str]:
        return list(self.df.columns)

    def get_column(self, col_name: str) -> PandasColumn:
        return PandasColumn(self.df[col_name])

    def get_columns(self, cols: List[str]) -> "PandasDataStructure":
        return PandasDataStructure(self.df[cols].copy())

    def create_column_from_column(
        self, col_name: str, col_values: PandasColumn
    ) -> None:
        self.df[col_name] = col_values.series

    def create_column_from_value(self, col_name: str, value) -> None:
        self.df[col_name] = value

    def set_column_from_column(self, col_name: str, col_values: PandasColumn) -> None:
        self.df[col_name] = col_values.series

    def set_column_from_value(self, col_name: str, value) -> None:
        self.df[col_name] = value

    def filter_from_column(self, col: PandasColumn) -> "PandasDataStructure":
        return PandasDataStructure(self.df[col.series].copy())

    def set_subset_from_column(
        self, col_name: str, col_filter: PandasColumn, col_values: PandasColumn
    ) -> None:
        self.df.loc[col_filter.series, col_name] = col_values.series

    def set_subset_from_value(
        self, col_name: str, col_filter: PandasColumn, value: Any
    ) -> None:
        self.df.loc[col_filter.series, col_name] = value

    def rename(self, names: Dict[str, str]) -> "PandasDataStructure":
        return PandasDataStructure(self.df.rename(columns=names).copy())

    def to_list_of_dicts(self) -> List[Dict]:
        return self.df.to_dict("records")

    @classmethod
    def from_list_of_dicts(
        cls, data: List[Dict[str, Any]], meta: Optional[Dict[str, str]]
    ) -> "AbstractDataStructure":
        meta = meta or {}
        df = DataFrame(data)
        for col in meta:
            if col in df:
                if meta[col] is not None:
                    df[col] = df[col].astype(meta[col])
            else:
                df[col] = None
        return PandasDataStructure(df)

    def count(self) -> int:
        return len(self.df)

    def _join(
        self,
        other: "PandasDataStructure",
        how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
        left_on: List[str] = None,
        right_on: List[str] = None,
        l_map: Dict[str, str] = None,
        r_map: Dict[str, str] = None,
    ) -> "PandasDataStructure":
        self_df = self.df.rename(columns=l_map)
        other_df = other.df.rename(columns=r_map)
        return PandasDataStructure(
            self_df.merge(
                other_df,
                how=how,
                left_on=[l_map.get(col, col) for col in left_on],
                right_on=[r_map.get(col, col) for col in right_on],
                suffixes=(False, False),
            ).copy()
        )

    def _group_by(
        self,
        keys: List[str],
        outputs: Dict[
            str,
            Tuple[
                Callable[["AbstractDataStructure"], Any],
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
    ) -> "PandasDataStructure":
        return PandasDataStructure(
            self.df.groupby(keys, as_index=False,).apply(
                lambda x: DataFrame(
                    [
                        {
                            **{key: max(x[key]) for key in keys},
                            **{
                                output: cast(agg(PandasDataStructure(x)))
                                for output, (agg, cast) in outputs.items()
                            },
                        }
                    ]
                )
            )
        )

    def _union(
        self, *others: "PandasDataStructure", all: bool
    ) -> "PandasDataStructure":
        ret = concat([self.df, *[other.df for other in others]])
        if all:
            return PandasDataStructure(ret)
        else:
            return PandasDataStructure(ret.drop_duplicates())

    def _distinct(self, keys: Optional[List[str]] = None) -> "PandasDataStructure":
        return PandasDataStructure(
            self.df[keys or self.list_columns()].copy().drop_duplicates()
        )

    def _apply(
        self, output_column: str, func: Callable[[Dict], Any], cast: Type
    ) -> "PandasDataStructure":
        df = self.df.copy()
        df[output_column] = df.apply(func, axis=1).astype(cast)
        return PandasDataStructure(df)

    def _sort(self, cols: List[str], asc: List[bool]) -> "PandasDataStructure":
        return PandasDataStructure(self.df.sort_values(by=cols, ascending=asc))

    def _limit(self, n: int) -> "PandasDataStructure":
        return PandasDataStructure(self.df.copy()[:n])

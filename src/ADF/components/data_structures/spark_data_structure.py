import datetime
from typing import Optional, List, Dict, Tuple, Callable, Any, Union, Type
from typing_extensions import Literal

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, when, col

from ADF.components.data_structures import (
    AbstractDataStructure,
    PandasDataStructure,
    ADSConcretizer,
    ADSConcretizerDict,
)
from ADF.components.columns import SparkColumn
from ADF.utils import sdf_apply


class SparkDataStructure(AbstractDataStructure):
    concretizations = ADSConcretizerDict(
        {
            list: ADSConcretizer(
                concretize=lambda ads: ads.df.rdd.map(
                    lambda row: row.asDict()
                ).collect(),
                extract_abstraction_params=lambda ads: {"spark": ads.df.sql_ctx},
                abstract=lambda l, params: SparkDataStructure(
                    params["spark"].createDataFrame(pd.DataFrame.from_records(l))
                ),
            ),
            dict: ADSConcretizer(
                concretize=lambda ads: ads.df.toPandas().to_dict(),
                extract_abstraction_params=lambda ads: {"spark": ads.df.sql_ctx},
                abstract=lambda d, params: SparkDataStructure(
                    params["spark"].createDataFrame(pd.DataFrame.from_dict(d))
                ),
            ),
            pd.DataFrame: ADSConcretizer(
                concretize=lambda ads: ads.df.toPandas(),
                extract_abstraction_params=lambda ads: {"spark": ads.df.sql_ctx},
                abstract=lambda df, params: SparkDataStructure(
                    params["spark"].createDataFrame(df)
                ),
            ),
            DataFrame: ADSConcretizer(
                concretize=lambda ads: ads.df,
                extract_abstraction_params=lambda ads: {},
                abstract=lambda df, params: SparkDataStructure(df),
            ),
        }
    )
    type_dict: Dict[str, Type] = {
        "str": str,
        "string": str,
        "int": int,
        "bigint": int,
        "integer": int,
        "float": float,
        "double": float,
        "timestamp": datetime.datetime.fromisoformat,
    }
    inverted_type_dict: Dict[Type, str] = {val: key for key, val in type_dict.items()}

    def __init__(self, df: DataFrame):
        super().__init__()
        self.df = df

    def list_columns(self) -> List[str]:
        return self.df.columns

    def spark_schema(self) -> Dict[str, str]:
        return {
            field.name: field.dataType.simpleString() for field in self.df.schema.fields
        }

    def get_column(self, col_name: str) -> SparkColumn:
        return SparkColumn(self.df, lambda x: x[col_name])

    def get_columns(self, cols: List[str]) -> "SparkDataStructure":
        return SparkDataStructure(self.df[cols])

    def create_column_from_column(self, col_name: str, col_values: SparkColumn) -> None:
        self.df = self.df.withColumn(col_name, col_values.column)

    def create_column_from_value(self, col_name: str, value) -> None:
        self.df = self.df.withColumn(col_name, lit(value))

    def set_column_from_column(self, col_name: str, col_values: SparkColumn) -> None:
        self.df = self.df.withColumn(col_name, col_values.column)

    def set_column_from_value(self, col_name: str, value) -> None:
        self.df = self.df.withColumn(col_name, lit(value))

    def filter_from_column(self, col: SparkColumn) -> "SparkDataStructure":
        return SparkDataStructure(self.df[col.column])

    def set_subset_from_column(
        self, col_name: str, col_filter: SparkColumn, col_values: SparkColumn
    ):
        return SparkDataStructure(
            self.df.withColumn(
                col_name,
                when(col_filter.column, col_values.column).otherwise(col(col_name)),
            )
        )

    def set_subset_from_value(self, col_name: str, col_filter: SparkColumn, value: Any):
        return SparkDataStructure(
            self.df.withColumn(
                col_name,
                when(col_filter.column, lit(value)).otherwise(col(col_name)),
            )
        )

    def rename(self, names: Dict[str, str]) -> "SparkDataStructure":
        df = self.df
        for old_name, new_name in names.items():
            df = df.withColumnRenamed(old_name, new_name)
        return SparkDataStructure(df)

    def to_list_of_dicts(self) -> List[Dict]:
        return self.df.rdd.map(lambda row: row.asDict()).collect()

    @classmethod
    def from_list_of_dicts(
        cls, data: List[Dict[str, Any]], meta: Optional[Dict[str, str]] = None
    ) -> "SparkDataStructure":
        meta = meta or {}
        spark = (
            SparkSession.builder.master("local")
            .appName("default-local-spark")
            .getOrCreate()
        )
        cols = data[0].keys() if len(data) else meta.keys()
        df = spark.createDataFrame(
            [
                {
                    key: cls.type_dict.get(meta.get(key, "string"), str)(val)
                    for key, val in row.items()
                }
                for row in data
            ],
            ", ".join(
                [
                    f"{col}: {(meta.get(col) or 'string') if meta.get(col) != 'null' else 'string'}"
                    for col in cols
                ]
            ),
        )
        for col in meta:
            if col not in cols:
                df.withColumn(col, lit(None))
        return SparkDataStructure(df)

    def count(self) -> int:
        return self.df.count()

    def _join(
        self,
        other: "SparkDataStructure",
        left_on: List[str],
        right_on: List[str],
        how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
        l_map: Dict[str, str] = None,
        r_map: Dict[str, str] = None,
    ) -> "SparkDataStructure":
        left_df = self.rename(l_map).df
        right_df = other.rename(r_map).df
        cond = lit(True)
        for left_col, right_col in zip(left_on, right_on):
            cond = cond & (
                left_df[l_map.get(left_col, left_col)]
                == right_df[r_map.get(right_col, right_col)]
            )
        join_df = left_df.join(right_df, cond, how)
        drop_columns = [
            column for column in left_df.columns if column in right_df.columns
        ]
        for column in drop_columns:
            join_df = join_df.drop(right_df[column])
        return SparkDataStructure(join_df)

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
    ) -> "SparkDataStructure":
        schema_map = {
            str: "string",
            int: "int",
            float: "double",
            # complex: "",
            datetime.datetime: "timestamp",
            datetime.date: "date",
            # datetime.timedelta: "",
        }
        self_schema = self.spark_schema()
        return SparkDataStructure(
            self.df.groupby(keys).applyInPandas(
                lambda x: pd.DataFrame(
                    [
                        {
                            **{key: max(x[key]) for key in keys},
                            **{
                                output: cast(agg(PandasDataStructure(x)))
                                for output, (agg, cast) in outputs.items()
                            },
                        }
                    ]
                ),
                ", ".join(
                    [f"{key} {self_schema[key]}" for key in keys]
                    + [
                        f"{output} {schema_map[agg[1]]}"
                        for output, agg in outputs.items()
                    ]
                ),
            )
        )

    def _union(self, *others: "SparkDataStructure", all: bool) -> "SparkDataStructure":
        df = self.df
        for other in others:
            df = df.unionByName(other.df)
        if not all:
            df = df.distinct()
        return SparkDataStructure(df)

    def _distinct(self, keys: Optional[List[str]] = None) -> "SparkDataStructure":
        return SparkDataStructure(self.df[keys].distinct())

    def _apply(
        self, output_column: str, func: Callable[[Dict], Any], cast: Type
    ) -> "SparkDataStructure":
        return SparkDataStructure(
            sdf_apply(
                sdf=self.df,
                target_col=output_column,
                inner=func,
                cast=cast,
            )
        )

    def _sort(self, cols: List[str], asc: List[bool]) -> "SparkDataStructure":
        return SparkDataStructure(self.df.sort(cols, asc=asc))

    def _limit(self, n: int) -> "SparkDataStructure":
        return SparkDataStructure(self.df.limit(n))

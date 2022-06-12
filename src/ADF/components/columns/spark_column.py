from typing import Callable, List, Any, Optional

from pyspark.sql import DataFrame, Column, functions
from pyspark.sql.functions import to_timestamp

from ADF.components.columns import AbstractDataColumn


class SparkColumn(AbstractDataColumn):
    def __init__(
        self,
        df: DataFrame,
        col_extractor: Optional[Callable[[DataFrame], Column]] = None,
    ):
        super().__init__()
        self.df = df
        self.col_extractor = col_extractor

    @property
    def column(self) -> Column:
        return self.col_extractor(self.df)

    def chain(self, chainable: Callable[[Column], Column]):
        return lambda x: chainable(self.col_extractor(x))

    def cast(self, t) -> "SparkColumn":
        return SparkColumn(self.df, self.chain(lambda x: x.cast(t)))

    def to_str(self) -> "SparkColumn":
        return self.cast("string")

    def to_int(self) -> "SparkColumn":
        return self.cast("integer")

    def to_float(self) -> "SparkColumn":
        return self.cast("float")

    def to_complex(self) -> "SparkColumn":
        raise NotImplementedError(
            f"Can't cast {self.__class__.__name__} to complex type."
        )

    def to_bool(self) -> "SparkColumn":
        return self.cast("boolean")

    def to_datetime_auto_convert(self) -> "SparkColumn":
        return SparkColumn(self.df, self.chain(lambda x: to_timestamp(x)))

    def to_datetime_from_timestamp(self) -> "SparkColumn":
        return SparkColumn(self.df, self.chain(lambda x: to_timestamp(x)))

    def to_datetime_from_format(self, datetime_format: str) -> "SparkColumn":
        for py_format, spark_format in {
            "%Y": "yyyy",
            "%m": "MM",
            "%D": "dd",
            "%H": "HH",
            "%M": "mm",
            "%S": "ss",
            "%f": "SSSSSS",
        }.keys():
            datetime_format = datetime_format.replace(py_format, spark_format)
        return SparkColumn(
            self.df, self.chain(lambda x: to_timestamp(x, format=datetime_format))
        )

    def to_date(self) -> "SparkColumn":
        return self.cast("date")

    def to_timedelta(self) -> "SparkColumn":
        raise NotImplementedError(f"Can't cast {self.__class__.__name__} to timedelta.")

    def col_operation(self, operation: Callable):
        return list(self.df.agg(operation(self.column)).collect()[0].asDict().values())[
            0
        ]

    def min(self) -> Any:
        return self.col_operation(functions.min)

    def max(self) -> Any:
        return self.col_operation(functions.max)

    def sum(self) -> Any:
        return self.col_operation(functions.sum)

    def mean(self) -> Any:
        return self.col_operation(functions.mean)

    def count(self) -> int:
        return self.df.count()

    def isin(self, comp: List) -> "SparkColumn":
        return SparkColumn(
            self.df,
            self.chain(lambda x: x.isin(comp)),
        )

    def default_binary_operator_col_left(
        self, op: Callable, other: "SparkColumn"
    ) -> "SparkColumn":
        if self.df is not other.df:
            raise ValueError(
                f"Can only sum {self.__class__.__name__} columns from the same dataframe."
            )
        return SparkColumn(
            self.df,
            lambda x: op(self.col_extractor(x), other.col_extractor(x)),
        )

    def default_binary_operator_col_right(
        self, op: Callable, other: "SparkColumn"
    ) -> "SparkColumn":
        if self.df is not other.df:
            raise ValueError(
                f"Can only sum {self.__class__.__name__} columns from the same dataframe."
            )
        return SparkColumn(
            self.df,
            lambda x: op(other.col_extractor(x), self.col_extractor(x)),
        )

    def default_binary_operator_val_left(self, op: Callable, other) -> "SparkColumn":
        return SparkColumn(
            self.df,
            self.chain(lambda x: op(x, other)),
        )

    def default_binary_operator_val_right(self, op: Callable, other) -> "SparkColumn":
        return SparkColumn(
            self.df,
            self.chain(lambda x: op(other, x)),
        )

    def default_unary_operator(self, op: Callable) -> "SparkColumn":
        return SparkColumn(
            self.df,
            self.chain(lambda x: op(x)),
        )

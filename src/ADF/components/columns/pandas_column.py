from typing import Callable, Any, List

from pandas import to_datetime
from pandas.api.types import is_numeric_dtype, is_string_dtype

from ADF.components.columns import AbstractDataColumn


class PandasColumn(AbstractDataColumn):
    def __init__(self, series):
        super().__init__()
        self.series = series

    # Data type methods

    def to_str(self) -> "PandasColumn":
        return PandasColumn(self.series.astype(str))

    def to_int(self) -> "PandasColumn":
        return PandasColumn(self.series.astype(int))

    def to_float(self) -> "PandasColumn":
        return PandasColumn(self.series.astype(float))

    def to_complex(self) -> "PandasColumn":
        return PandasColumn(self.series.astype(complex))

    def to_bool(self) -> "PandasColumn":
        return PandasColumn(self.series.astype(bool))

    def to_datetime_auto_convert(self) -> "AbstractDataColumn":
        if is_numeric_dtype(self.series):
            return PandasColumn(to_datetime(self.series * 1e9))
        return PandasColumn(to_datetime(self.series))

    def to_datetime_from_timestamp(self) -> "PandasColumn":
        return PandasColumn(to_datetime(self.series * 1e9))

    def to_datetime_from_format(self, datetime_format: str) -> "PandasColumn":
        return PandasColumn(to_datetime(self.series, format=datetime_format))

    def to_date(self) -> "PandasColumn":
        raise NotImplementedError

    def to_timedelta(self) -> "PandasColumn":
        raise NotImplementedError

    # Column aggregations

    def min(self) -> Any:
        return self.series.min()

    def max(self) -> Any:
        return self.series.max()

    def sum(self) -> Any:
        return self.series.sum()

    def mean(self) -> Any:
        return self.series.mean()

    def count(self) -> int:
        return len(self.series)

    # Column operations

    def isin(self, comp: List) -> "PandasColumn":
        return PandasColumn(self.series.isin(comp))

    # Binary operators

    def default_binary_operator_col_right(
        self, op: Callable, other: "PandasColumn"
    ) -> "PandasColumn":
        return PandasColumn(op(self.series, other.series))

    def default_binary_operator_col_left(
        self, op: Callable, other: "PandasColumn"
    ) -> "PandasColumn":
        return PandasColumn(op(other.series, self.series))

    def default_binary_operator_val_left(self, op: Callable, other) -> "PandasColumn":
        return PandasColumn(op(self.series, other))

    def default_binary_operator_val_right(self, op: Callable, other) -> "PandasColumn":
        return PandasColumn(op(other, self.series))

    # Unary operators

    def default_unary_operator(self, op: Callable) -> "PandasColumn":
        return PandasColumn(op(self.series))

import numbers
import datetime

from typing import Callable, Any, List, Optional, Type

from ADF.components.columns import AbstractDataColumn


class ListColumn(AbstractDataColumn):
    def __init__(self, data: List[Any], meta: Optional[Type] = None):
        super().__init__()
        self.data = data
        self.meta = meta

    def cast(self, caster: Callable, meta: Optional[Type] = None) -> "ListColumn":
        meta = meta or caster
        return ListColumn(
            [caster(e) if e is not None else None for e in self.data], meta
        )

    def to_str(self) -> "ListColumn":
        return self.cast(str)

    def to_int(self) -> "ListColumn":
        return self.cast(int)

    def to_float(self) -> "ListColumn":
        return self.cast(float)

    def to_complex(self) -> "ListColumn":
        return self.cast(complex)

    def to_bool(self) -> "ListColumn":
        return self.cast(bool)

    def to_datetime_auto_convert(self) -> "ListColumn":
        return self.cast(
            lambda x: datetime.datetime.fromtimestamp(x)
            if isinstance(x, numbers.Number)
            else datetime.datetime.fromisoformat(str(x)),
            datetime.datetime,
        )

    def to_datetime_from_timestamp(self) -> "ListColumn":
        return self.cast(datetime.datetime.fromtimestamp, datetime.datetime)

    def to_datetime_from_format(self, datetime_format: str) -> "ListColumn":
        return self.cast(
            lambda x: datetime.datetime.strptime(x, datetime_format), datetime.datetime
        )

    def to_date(self) -> "ListColumn":
        raise NotImplementedError

    def to_timedelta(self) -> "ListColumn":
        raise NotImplementedError

    def min(self) -> Any:
        return min(self.data)

    def max(self) -> Any:
        return max(self.data)

    def sum(self) -> Any:
        return sum(self.data)

    def mean(self) -> Any:
        return sum(self.data) / len(self.data)

    def count(self) -> int:
        return len(self.data)

    def isin(self, comp: List) -> "ListColumn":
        return ListColumn([e in comp for e in self.data])

    def default_binary_operator_col_left(
        self, op: Callable, other: "ListColumn"
    ) -> "ListColumn":
        if len(self.data) != len(other.data):
            raise ValueError(
                f"Cannot sum list columns of different lengths : {len(self.data)} VS {len(other.data)}."
            )
        return ListColumn(
            [op(self.data[i], other.data[i]) for i in range(len(self.data))]
        )

    def default_binary_operator_col_right(
        self, op: Callable, other: "ListColumn"
    ) -> "ListColumn":
        if len(self.data) != len(other.data):
            raise ValueError(
                f"Cannot sum list columns of different lengths : {len(self.data)} VS {len(other.data)}."
            )
        return ListColumn(
            [op(other.data[i], self.data[i]) for i in range(len(self.data))]
        )

    def default_binary_operator_val_left(self, op: Callable, other) -> "ListColumn":
        return ListColumn([op(e, other) for e in self.data])

    def default_binary_operator_val_right(self, op: Callable, other) -> "ListColumn":
        return ListColumn([op(other, e) for e in self.data])

    def default_unary_operator(self, op: Callable) -> "ListColumn":
        return ListColumn([op(e) for e in self.data])

import datetime
import operator

from abc import ABC
from typing import Callable, Any, List, Union

from ADF.exceptions import UnsupportedType


class AbstractDataColumn(ABC):
    def __init__(self):
        pass

    # Data type methods

    def as_type(self, t, **kwargs):
        if t is str:
            return self.to_str()
        elif t is int:
            return self.to_int()
        elif t is float:
            return self.to_float()
        elif t is complex:
            return self.to_complex()
        elif t is bool:
            return self.to_bool()
        elif t is datetime.datetime:
            return self.to_datetime(**kwargs)
        elif t is datetime.date:
            return self.to_date()
        elif t is datetime.timedelta:
            return self.to_timedelta()
        else:
            raise UnsupportedType(t, self.__class__.__name__)

    def to_str(self) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_int(self) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_float(self) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_complex(self) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_bool(self) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_datetime(
        self,
        auto_convert: bool = True,
        as_timestamp: bool = False,
        datetime_format="%Y-%m-%D %H:%M:%S.%f",
    ) -> "AbstractDataColumn":
        if auto_convert:
            return self.to_datetime_auto_convert()
        elif as_timestamp:
            return self.to_datetime_from_timestamp()
        else:
            return self.to_datetime_from_format(datetime_format)

    def to_datetime_auto_convert(self) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_datetime_from_timestamp(self) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_datetime_from_format(self, format: str) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_date(self) -> "AbstractDataColumn":
        raise NotImplementedError

    def to_timedelta(self) -> "AbstractDataColumn":
        raise NotImplementedError

    # Column aggregations

    def min(self) -> Any:
        raise NotImplementedError

    def max(self) -> Any:
        raise NotImplementedError

    def sum(self) -> Any:
        raise NotImplementedError

    def mean(self) -> Any:
        raise NotImplementedError

    def count(self) -> int:
        raise NotImplementedError

    def __len__(self) -> int:
        return self.count()

    def __bool__(self) -> bool:
        return len(self) != 0

    # Column operations

    def isin(self, comp: List) -> "AbstractDataColumn":
        raise NotImplementedError

    # Binary operators

    def default_binary_operator_col_left(
        self, op: Callable, other: "AbstractDataColumn"
    ) -> "AbstractDataColumn":
        raise NotImplementedError

    def default_binary_operator_col_right(
        self, op: Callable, other: "AbstractDataColumn"
    ) -> "AbstractDataColumn":
        raise NotImplementedError

    def default_binary_operator_val_left(
        self, op: Callable, other
    ) -> "AbstractDataColumn":
        raise NotImplementedError

    def default_binary_operator_val_right(
        self, op: Callable, other
    ) -> "AbstractDataColumn":
        raise NotImplementedError

    def _handle_binary_operator(
        self, op: Callable, other: Union["AbstractDataColumn", Any], left: bool
    ):
        if isinstance(other, AbstractDataColumn):
            if op is operator.add and left:
                return self.add_col(other)
            if op is operator.add and not left:
                return self.radd_col(other)
            if op is operator.sub and left:
                return self.sub_col(other)
            if op is operator.sub and not left:
                return self.rsub_col(other)
            if op is operator.mul and left:
                return self.mul_col(other)
            if op is operator.mul and not left:
                return self.rmul_col(other)
            if op is operator.floordiv and left:
                return self.floordiv_col(other)
            if op is operator.floordiv and not left:
                return self.rfloordiv_col(other)
            if op is operator.truediv and left:
                return self.truediv_col(other)
            if op is operator.truediv and not left:
                return self.rtruediv_col(other)
            if op is operator.mod and left:
                return self.mod_col(other)
            if op is operator.mod and not left:
                return self.rmod_col(other)
            if op is operator.pow and left:
                return self.pow_col(other)
            if op is operator.pow and not left:
                return self.rpow_col(other)
            if op is operator.eq and left:
                return self.eq_col(other)
            if op is operator.ne and left:
                return self.ne_col(other)
            if op is operator.lt and left:
                return self.lt_col(other)
            if op is operator.le and left:
                return self.le_col(other)
            if op is operator.gt and left:
                return self.gt_col(other)
            if op is operator.ge and left:
                return self.ge_col(other)
            if op is operator.and_ and left:
                return self.and_col(other)
            if op is operator.and_ and not left:
                return self.rand_col(other)
            if op is operator.or_ and left:
                return self.or_col(other)
            if op is operator.or_ and not left:
                return self.ror_col(other)
            raise ValueError(f"Unknown binary operator {str(op)}.")
        else:
            if op is operator.add and left:
                return self.add_val(other)
            if op is operator.add and not left:
                return self.radd_val(other)
            if op is operator.sub and left:
                return self.sub_val(other)
            if op is operator.sub and not left:
                return self.rsub_val(other)
            if op is operator.mul and left:
                return self.mul_val(other)
            if op is operator.mul and not left:
                return self.rmul_val(other)
            if op is operator.floordiv and left:
                return self.floordiv_val(other)
            if op is operator.floordiv and not left:
                return self.rfloordiv_val(other)
            if op is operator.truediv and left:
                return self.truediv_val(other)
            if op is operator.truediv and not left:
                return self.rtruediv_val(other)
            if op is operator.mod and left:
                return self.mod_val(other)
            if op is operator.mod and not left:
                return self.rmod_val(other)
            if op is operator.pow and left:
                return self.pow_val(other)
            if op is operator.pow and not left:
                return self.rpow_val(other)
            if op is operator.eq and left:
                return self.eq_val(other)
            if op is operator.ne and left:
                return self.ne_val(other)
            if op is operator.lt and left:
                return self.lt_val(other)
            if op is operator.le and left:
                return self.le_val(other)
            if op is operator.gt and left:
                return self.gt_val(other)
            if op is operator.ge and left:
                return self.ge_val(other)
            if op is operator.and_ and left:
                return self.and_val(other)
            if op is operator.and_ and not left:
                return self.rand_val(other)
            if op is operator.or_ and left:
                return self.or_val(other)
            if op is operator.or_ and not left:
                return self.ror_val(other)
            raise ValueError(f"Unknown binary operator {str(op)}.")

    def add_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.add, other)

    def radd_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.add, other)

    def sub_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.sub, other)

    def rsub_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.sub, other)

    def mul_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.mul, other)

    def rmul_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.mul, other)

    def floordiv_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.floordiv, other)

    def rfloordiv_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.floordiv, other)

    def truediv_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.truediv, other)

    def rtruediv_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.truediv, other)

    def mod_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.mod, other)

    def rmod_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.mod, other)

    def pow_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.pow, other)

    def rpow_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.pow, other)

    def eq_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.eq, other)

    def ne_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.ne, other)

    def lt_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.lt, other)

    def le_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.le, other)

    def gt_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.gt, other)

    def ge_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.ge, other)

    def and_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.and_, other)

    def rand_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.and_, other)

    def or_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_left(operator.or_, other)

    def ror_col(self, other: "AbstractDataColumn"):
        return self.default_binary_operator_col_right(operator.or_, other)

    def add_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.add, other)

    def radd_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.add, other)

    def sub_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.sub, other)

    def rsub_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.sub, other)

    def mul_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.mul, other)

    def rmul_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.mul, other)

    def floordiv_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.floordiv, other)

    def rfloordiv_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.floordiv, other)

    def truediv_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.truediv, other)

    def rtruediv_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.truediv, other)

    def mod_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.mod, other)

    def rmod_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.mod, other)

    def pow_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.pow, other)

    def rpow_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.pow, other)

    def eq_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.eq, other)

    def ne_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.ne, other)

    def lt_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.lt, other)

    def le_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.le, other)

    def gt_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.gt, other)

    def ge_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.ge, other)

    def and_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.and_, other)

    def rand_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.and_, other)

    def or_val(self, other: Any):
        return self.default_binary_operator_val_left(operator.or_, other)

    def ror_val(self, other: Any):
        return self.default_binary_operator_val_right(operator.or_, other)

    def __add__(self, other):
        return self._handle_binary_operator(operator.add, other, left=True)

    def __radd__(self, other):
        return self._handle_binary_operator(operator.add, other, left=False)

    def __sub__(self, other):
        return self._handle_binary_operator(operator.sub, other, left=True)

    def __rsub__(self, other):
        return self._handle_binary_operator(operator.sub, other, left=False)

    def __mul__(self, other):
        return self._handle_binary_operator(operator.mul, other, left=True)

    def __rmul__(self, other):
        return self._handle_binary_operator(operator.mul, other, left=False)

    def __floordiv__(self, other):
        return self._handle_binary_operator(operator.floordiv, other, left=True)

    def __rfloordiv__(self, other):
        return self._handle_binary_operator(operator.floordiv, other, left=False)

    def __truediv__(self, other):
        return self._handle_binary_operator(operator.truediv, other, left=True)

    def __rtruediv__(self, other):
        return self._handle_binary_operator(operator.truediv, other, left=False)

    def __mod__(self, other):
        return self._handle_binary_operator(operator.mod, other, left=True)

    def __rmod__(self, other):
        return self._handle_binary_operator(operator.mod, other, left=False)

    def __pow__(self, other):
        return self._handle_binary_operator(operator.pow, other, left=True)

    def __rpow__(self, other):
        return self._handle_binary_operator(operator.pow, other, left=False)

    def __eq__(self, other):
        return self._handle_binary_operator(operator.eq, other, left=True)

    def __ne__(self, other):
        return self._handle_binary_operator(operator.ne, other, left=True)

    def __lt__(self, other):
        return self._handle_binary_operator(operator.lt, other, left=True)

    def __le__(self, other):
        return self._handle_binary_operator(operator.le, other, left=True)

    def __gt__(self, other):
        return self._handle_binary_operator(operator.gt, other, left=True)

    def __ge__(self, other):
        return self._handle_binary_operator(operator.ge, other, left=True)

    def __and__(self, other):
        return self._handle_binary_operator(operator.and_, other, left=True)

    def __rand__(self, other):
        return self._handle_binary_operator(operator.and_, other, left=False)

    def __or__(self, other):
        return self._handle_binary_operator(operator.or_, other, left=True)

    def __ror__(self, other):
        return self._handle_binary_operator(operator.or_, other, left=False)

    # Unary operators

    def default_unary_operator(self, op: Callable) -> "AbstractDataColumn":
        raise NotImplementedError

    def _handle_unary_operator(self, op: Callable):
        if op is operator.neg:
            return self.neg_unary()
        if op is operator.pos:
            return self.pos_unary()
        if op is operator.abs:
            return self.abs_unary()
        if op is operator.invert:
            return self.invert_unary()
        raise ValueError(f"Unknown unary operator {str(op)}.")

    def neg_unary(self):
        return self.default_unary_operator(operator.neg)

    def pos_unary(self):
        return self.default_unary_operator(operator.pos)

    def abs_unary(self):
        return self.default_unary_operator(operator.abs)

    def invert_unary(self):
        return self.default_unary_operator(operator.invert)

    def __neg__(self):
        return self._handle_unary_operator(operator.neg)

    def __pos__(self):
        return self._handle_unary_operator(operator.pos)

    def __abs__(self):
        return self._handle_unary_operator(operator.abs)

    def __invert__(self):
        return self._handle_unary_operator(operator.invert)

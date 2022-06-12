from typing import Callable, List, Any

from sqlalchemy import func, String, Integer, Float, Boolean, DateTime, Date
from sqlalchemy.orm import Session
from sqlalchemy.sql import Subquery

from ADF.components.columns import AbstractDataColumn
from ADF.exceptions import MismatchedSQLSubqueries, UnhandledGroupOperation


class SQLColumn(AbstractDataColumn):
    def __init__(
        self,
        session: Session,
        subquery: Subquery,
        col_extractor: Callable,
    ):
        super().__init__()
        self.session = session
        self.subquery = subquery
        self.col_extractor = col_extractor

    @property
    def column(self):
        return self.col_extractor(self.subquery)

    def chain(self, chainable: Callable):
        return lambda x: chainable(self.col_extractor(x))

    def cast(self, t) -> "SQLColumn":
        return SQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=self.chain(lambda x: x.cast(t)),
        )

    def to_str(self) -> "SQLColumn":
        return self.cast(String)

    def to_int(self) -> "SQLColumn":
        return self.cast(Integer)

    def to_float(self) -> "SQLColumn":
        return self.cast(Float)

    def to_complex(self) -> "SQLColumn":
        raise NotImplementedError(
            f"Can't cast {self.__class__.__name__} to complex type."
        )

    def to_bool(self) -> "SQLColumn":
        return self.cast(Boolean)

    def to_datetime_auto_convert(self) -> "SQLColumn":
        return self.cast(DateTime)

    def to_datetime_from_timestamp(self) -> "SQLColumn":
        return self.cast(DateTime)

    def to_datetime_from_format(self, format: str) -> "SQLColumn":
        return self.cast(DateTime)

    def to_date(self) -> "SQLColumn":
        return self.cast(Date)

    def to_timedelta(self) -> "SQLColumn":
        raise NotImplementedError(
            f"Can't cast {self.__class__.__name__} to time delta."
        )

    def col_operation(self, operation: Callable):
        return self.session.query(operation(self.column).label("result")).one()[
            "result"
        ]

    def min(self) -> Any:
        return self.col_operation(func.min)

    def max(self) -> Any:
        return self.col_operation(func.max)

    def sum(self) -> Any:
        return self.col_operation(func.sum)

    def mean(self) -> Any:
        return self.col_operation(func.avg)

    def count(self) -> int:
        return self.col_operation(func.count)

    def isin(self, comp: List) -> "SQLColumn":
        return SQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=self.chain(lambda x: x.in_(comp)),
        )

    def default_binary_operator_col_left(
        self, op: Callable, other: "SQLColumn"
    ) -> "SQLColumn":
        if self.subquery is not other.subquery:
            raise MismatchedSQLSubqueries(
                self, other, f"{self.__class__.__name__} binary operator {str(op)}"
            )
        return SQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=lambda x: op(self.col_extractor(x), other.col_extractor(x)),
        )

    def default_binary_operator_col_right(
        self, op: Callable, other: "SQLColumn"
    ) -> "SQLColumn":
        if self.subquery is not other.subquery:
            raise MismatchedSQLSubqueries(
                self, other, f"{self.__class__.__name__} binary operator {str(op)}"
            )
        return SQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=lambda x: op(other.col_extractor(x), self.col_extractor(x)),
        )

    def default_binary_operator_val_left(self, op: Callable, other) -> "SQLColumn":
        return SQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=self.chain(lambda x: op(x, other)),
        )

    def default_binary_operator_val_right(self, op: Callable, other) -> "SQLColumn":
        return SQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=self.chain(lambda x: op(other, x)),
        )

    def default_unary_operator(self, op: Callable) -> "SQLColumn":
        return SQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=self.chain(lambda x: op(x)),
        )


class GroupedSQLColumn(SQLColumn):
    def __init__(
        self,
        session: Session,
        subquery: Subquery,
        col_extractor: Callable,
    ):
        super().__init__(
            session=session,
            subquery=subquery,
            col_extractor=col_extractor,
        )

    def cast(self, t) -> "GroupedSQLColumn":
        return GroupedSQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=self.chain(lambda x: x.cast(t)),
        )

    def isin(self, comp: List) -> "GroupedSQLColumn":
        raise UnhandledGroupOperation(self, "isin")

    def col_operation(self, operation: Callable):
        return operation(self.column)

    def to_complex(self) -> "SQLColumn":
        raise NotImplementedError(f"Can't cast {self.__class__.__name__} to complex.")

    def to_timedelta(self) -> "SQLColumn":
        raise NotImplementedError(
            f"Can't cast {self.__class__.__name__} to time delta."
        )

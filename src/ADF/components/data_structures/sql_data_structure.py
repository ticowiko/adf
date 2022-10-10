import datetime

from typing import Optional, List, Dict, Tuple, Callable, Any, Union, Type
from typing_extensions import Literal

from sqlalchemy import func, distinct, null, desc as sql_desc, asc as sql_asc
from sqlalchemy.orm import Session
from sqlalchemy.sql import literal_column, Subquery, case, literal, text

from ADF.components.data_structures import (
    AbstractDataStructure,
    ADSConcretizerDict,
    ADSConcretizer,
)
from ADF.components.columns import SQLColumn, GroupedSQLColumn
from ADF.components.data_structures import RawSQLConcretization
from ADF.exceptions import MismatchedSQLSubqueries, UnhandledGroupOperation
from ADF.utils import sql_type_map, sql_inverse_type_map


class SQLDataStructure(AbstractDataStructure):
    concretizations = ADSConcretizerDict(
        {
            RawSQLConcretization: ADSConcretizer(
                concretize=lambda ads: RawSQLConcretization(
                    sql=ads.query_string(),
                    cols={
                        col_name: sql_inverse_type_map[col.type.__class__]
                        for col_name, col in ads.cols.items()
                    },
                ),
                extract_abstraction_params=lambda ads: {"session": ads.session},
                abstract=lambda raw, params: SQLDataStructure.from_raw_sql_concretization(
                    raw, params
                ),
            )
        }
    )

    @classmethod
    def from_raw_sql_concretization(
        cls, raw: RawSQLConcretization, params: Dict[str, Any]
    ) -> "SQLDataStructure":
        subquery = (
            text(raw.sql)
            .columns(**{key: sql_type_map[val] for key, val in raw.cols.items()})
            .subquery()
        )
        return SQLDataStructure(
            session=params["session"],
            subquery=subquery,
            cols={col: subquery.c[col] for col in raw.cols},
        )

    def __init__(self, session: Session, subquery: Subquery, cols: Dict):
        super().__init__()
        self.session = session
        self.subquery = subquery
        self.cols = cols

    def query_string(self, labels: Optional[List] = None):
        dialect = self.session.get_bind().dialect
        ret = str(
            self.query(labels).statement.compile(
                dialect=dialect, compile_kwargs={"literal_binds": True}
            )
        )
        try:
            from pyathena.sqlalchemy_athena import AthenaDialect

            if isinstance(dialect, AthenaDialect):
                ret = ret.replace("AS STRING", "AS VARCHAR")
        except ModuleNotFoundError:
            pass
        return ret

    def query(self, labels: Optional[List] = None):
        labels = labels or list(self.cols.keys())
        return self.session.query(*[self.cols[label].label(label) for label in labels])

    def list_columns(self) -> List[str]:
        return list(self.cols.keys())

    def get_column(self, col_name: str) -> SQLColumn:
        return SQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=lambda x: self.cols[col_name],
        )

    def get_columns(self, cols: List[str]) -> "SQLDataStructure":
        return SQLDataStructure(
            session=self.session,
            subquery=self.subquery,
            cols={label: col for label, col in self.cols.items() if label in cols},
        )

    def create_column_from_column(self, col_name: str, col_values: SQLColumn) -> None:
        if self.subquery is not col_values.subquery:
            raise MismatchedSQLSubqueries(self, col_values, "create_column_from_column")
        self.cols[col_name] = col_values.column

    def create_column_from_value(self, col_name: str, value) -> None:
        self.cols[col_name] = (
            literal_column(f"'{value}'") if value is not None else null()
        ).label(col_name)

    def set_column_from_column(self, col_name: str, col_values: SQLColumn) -> None:
        if self.subquery is not col_values.subquery:
            raise MismatchedSQLSubqueries(self, col_values, "set_column_from_column")
        self.cols[col_name] = col_values.column

    def set_column_from_value(self, col_name: str, value) -> None:
        self.cols[col_name] = (
            literal_column(f"'{value}'") if value is not None else null()
        ).label(col_name)

    def filter_from_column(self, col: SQLColumn) -> "SQLDataStructure":
        if self.subquery is not col.subquery:
            raise MismatchedSQLSubqueries(self, col, "filter_from_column")
        subquery = self.query().filter(col.column).subquery()
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={column: subquery.c[column] for column in self.cols},
        )

    def set_subset_from_column(
        self, col_name: str, col_filter: SQLColumn, col_values: SQLColumn
    ):
        if self.subquery is not col_filter.subquery:
            raise MismatchedSQLSubqueries(self, col_filter, "filter_from_column")
        if self.subquery is not col_values.subquery:
            raise MismatchedSQLSubqueries(self, col_filter, "filter_from_column")
        self.cols[col_name] = case(
            [(col_filter.column, col_values.column)], else_=self.cols[col_name]
        )

    def set_subset_from_value(self, col_name: str, col_filter: SQLColumn, value: Any):
        self.cols[col_name] = case(
            [(col_filter.column, literal_column(str(value)))], else_=self.cols[col_name]
        )

    def rename(self, names: Dict[str, str]) -> "SQLDataStructure":
        return SQLDataStructure(
            session=self.session,
            subquery=self.subquery,
            cols={names.get(label, label): col for label, col in self.cols.items()},
        )

    def to_list_of_dicts(self) -> List[Dict]:
        cols = self.list_columns()
        return [{col: getattr(result, col) for col in cols} for result in self.query()]

    @classmethod
    def from_list_of_dicts(
        cls, data: List[Dict[str, Any]], meta: Optional[Dict[str, str]]
    ) -> "SQLDataStructure":
        raise NotImplementedError(
            f"{cls.__name__} cannot be instantiated from list of dicts."
        )

    def count(self) -> int:
        return self.query().count()

    def _join(
        self,
        other: "SQLDataStructure",
        left_on: List[str],
        right_on: List[str],
        how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
        l_map: Dict[str, str] = None,
        r_map: Dict[str, str] = None,
    ) -> "SQLDataStructure":
        output_cols = list(l_map.values()) + list(r_map.values())
        if how == "cross":
            subquery = self.session.query(
                *[col.label(l_map[label]) for label, col in self.cols.items()],
                *[col.label(r_map[label]) for label, col in other.cols.items()],
            ).subquery()
            return SQLDataStructure(
                session=self.session,
                subquery=subquery,
                cols={col: subquery.c[col] for col in output_cols},
            )
        elif how in ["left", "right", "outer", "inner"]:
            condition = literal(True)
            for left_col, right_col in zip(left_on, right_on):
                condition = condition & (self.cols[left_col] == other.cols[right_col])
            subquery = (
                self.session.query(
                    *[col.label(l_map[label]) for label, col in self.cols.items()],
                    *[
                        col.label(r_map[label])
                        for label, col in other.cols.items()
                        if r_map[label] not in l_map.values()
                    ],
                )
                .join(
                    self.subquery if how == "right" else other.subquery,
                    condition,
                    isouter=(how in ["left", "right"]),
                    full=(how == "full"),
                )
                .subquery()
            )
            return SQLDataStructure(
                session=self.session,
                subquery=subquery,
                cols={col: subquery.c[col] for col in output_cols},
            )

    def _group_by(
        self,
        keys: List[str],
        outputs: Dict[
            str,
            Tuple[
                Callable[["GroupedSQLDataStructure"], Any],
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
    ) -> "SQLDataStructure":
        subquery = (
            self.session.query(
                *[self.cols[key] for key in keys],
                *[
                    agg(GroupedSQLDataStructure.from_sql_ds(self))
                    .cast(sql_type_map[cast])
                    .label(output)
                    for output, (agg, cast) in outputs.items()
                ],
            )
            .group_by(*[self.cols[key] for key in keys])
            .subquery()
        )
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={key: subquery.c[key] for key in list(keys) + list(outputs.keys())},
        )

    def _union(self, *others: "SQLDataStructure", all: bool) -> "SQLDataStructure":
        cols = self.list_columns()
        if all:
            subquery = (
                self.query(cols)
                .union_all(*[other.query(cols) for other in others])
                .subquery()
            )
            return SQLDataStructure(
                session=self.session,
                subquery=subquery,
                cols={col: subquery.c[col] for col in cols},
            )
        else:
            subquery = (
                self.query(cols)
                .union_all(*[other.query(cols) for other in others])
                .distinct()
                .subquery()
            )
            return SQLDataStructure(
                session=self.session,
                subquery=subquery,
                cols={col: subquery.c[col] for col in cols},
            )

    def _distinct(self, keys: Optional[List[str]] = None) -> "SQLDataStructure":
        subquery = self.query(keys).distinct().subquery()
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={col: subquery.c[col] for col in keys},
        )

    def _apply(
        self, output_column: str, func: Callable[[Dict], Any], cast: Type
    ) -> "SQLDataStructure":
        raise NotImplementedError(f"{self.__class__.__name__} cannot apply UDFs.")

    def _sort(self, cols: List[str], asc: List[bool]) -> "SQLDataStructure":
        query = self.query()
        for col, is_asc in zip(cols, asc):
            if is_asc:
                query = query.order_by(sql_asc(self.cols[col]))
            else:
                query = query.order_by(sql_desc(self.cols[col]))
        subquery = query.subquery()
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={col: subquery.c[col] for col in self.cols},
        )

    def _limit(self, n: int) -> "SQLDataStructure":
        subquery = self.query().limit(n).subquery()
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={col: subquery.c[col] for col in self.cols},
        )


class GroupedSQLDataStructure(SQLDataStructure):
    @classmethod
    def from_sql_ds(cls, sql_ds: SQLDataStructure):
        return cls(
            session=sql_ds.session,
            subquery=sql_ds.subquery,
            cols=sql_ds.cols,
        )

    def get_column(self, col_name: str) -> GroupedSQLColumn:
        return GroupedSQLColumn(
            session=self.session,
            subquery=self.subquery,
            col_extractor=lambda x: x.c[col_name],
        )

    def get_columns(self, cols: List[str]) -> "GroupedSQLDataStructure":
        raise UnhandledGroupOperation(self, "get_column")

    def create_column_from_column(self, col_name: str, col_values: SQLColumn) -> None:
        raise UnhandledGroupOperation(self, "create_column_from_column")

    def create_column_from_value(self, col_name: str, value) -> None:
        raise UnhandledGroupOperation(self, "create_column_from_value")

    def set_column_from_column(self, col_name: str, col_values: SQLColumn) -> None:
        raise UnhandledGroupOperation(self, "set_column_from_column")

    def set_column_from_value(self, col_name: str, value) -> None:
        raise UnhandledGroupOperation(self, "set_column_from_value")

    def filter_from_column(self, col: SQLColumn) -> "GroupedSQLDataStructure":
        raise UnhandledGroupOperation(self, "filter_from_column")

    def set_subset_from_column(
        self, col_name: str, col_filter: SQLColumn, col_values: SQLColumn
    ):
        raise UnhandledGroupOperation(self, "set_subset_from_column")

    def set_subset_from_value(self, col_name: str, col_filter: SQLColumn, value):
        raise UnhandledGroupOperation(self, "set_subset_from_value")

    def rename(self, names: Dict[str, str]) -> "GroupedSQLDataStructure":
        raise UnhandledGroupOperation(self, "rename")

    def to_list_of_dicts(self) -> List[Dict]:
        raise UnhandledGroupOperation(self, "to_list_of_dicts")

    @classmethod
    def from_list_of_dicts(
        cls, data: List[Dict[str, Any]], meta: Optional[Dict[str, str]]
    ) -> "SQLDataStructure":
        raise NotImplementedError(
            f"{cls.__name__} cannot be instantiated from list of dicts."
        )

    def count(self) -> int:
        if self.cols:
            return func.count()
        else:
            return func.count(self.subquery)

    def _join(
        self,
        other: "GroupedSQLDataStructure",
        left_on: List[str],
        right_on: List[str],
        how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
        l_map: Dict[str, str] = None,
        r_map: Dict[str, str] = None,
    ) -> "GroupedSQLDataStructure":
        raise UnhandledGroupOperation(self, "join")

    def _group_by(
        self,
        keys: List[str],
        outputs: Dict[
            str,
            Tuple[
                Callable[["GroupedSQLDataStructure"], Any],
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
    ) -> "GroupedSQLDataStructure":
        raise UnhandledGroupOperation(self, "group_by")

    def _union(
        self, *others: List["GroupedSQLDataStructure"], all: bool
    ) -> "GroupedSQLDataStructure":
        raise UnhandledGroupOperation(self, "union")

    def _distinct(self, keys: Optional[List[str]] = None) -> "GroupedSQLDataStructure":
        return GroupedSQLDataStructure(
            session=self.session,
            subquery=distinct(*[self.cols[key] for key in keys]),
            cols={},
        )

    def _apply(
        self, output_column: str, func: Callable[[Dict], Any], cast: Type
    ) -> "GroupedSQLDataStructure":
        raise NotImplementedError(f"{self.__class__.__name__} cannot apply UDFs.")

    def _sort(self, cols: List[str], asc: List[bool]) -> "AbstractDataStructure":
        raise UnhandledGroupOperation(self, "sort")

    def _limit(self, n: int) -> "AbstractDataStructure":
        raise UnhandledGroupOperation(self, "limit")

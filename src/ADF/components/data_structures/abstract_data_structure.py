import datetime

from abc import ABC, abstractmethod

from typing import Union, List, Tuple, Dict, Callable, Any, Type, Optional
from typing_extensions import Literal

from ADF.config import ADFGlobalConfig
from ADF.exceptions import (
    ColumnNotFound,
    InputError,
    JoinError,
    GroupByError,
    UnionError,
    DistinctError,
    UnhandledConcretization,
)
from ADF.components.columns import AbstractDataColumn


class ADSConcretizer(ABC):
    def __init__(
        self,
        concretize: Callable[["AbstractDataStructure"], Any],
        extract_abstraction_params: Callable[["AbstractDataStructure"], Dict[str, Any]],
        abstract: Callable[[Any, Dict[str, Any]], "AbstractDataStructure"],
    ):
        self.concretize = concretize
        self.extract_abstraction_params = extract_abstraction_params
        self.abstract = abstract


class ADSConcretizerDict:
    def __init__(self, concretizers: Dict[Type, ADSConcretizer]):
        self.concretizers = concretizers

    def __getitem__(self, concrete_class):
        for key, concretizer in self.concretizers.items():
            if issubclass(concrete_class, key):
                return concretizer
        raise UnhandledConcretization(concrete_class=concrete_class)


class RawSQLConcretization:
    def __init__(
        self,
        sql: str,
        cols: Dict[
            str,
            Union[
                Type[str],
                Type[int],
                Type[float],
                Type[complex],
                Type[bool],
                Type[datetime.datetime],
                Type[datetime.date],
                Type[datetime.timedelta],
                Type[type(None)],
            ],
        ],
    ):
        self.sql = sql.strip().rstrip(";").strip()
        self.cols = cols


class AbstractDataStructure(ABC):
    concretizations: ADSConcretizerDict = ADSConcretizerDict({})

    def __init__(self):
        pass

    @abstractmethod
    def list_columns(self) -> List[str]:
        pass

    def col_exists(self, col_name: str) -> bool:
        return col_name in self.list_columns()

    def prune_tech_cols(self) -> "AbstractDataStructure":
        return self[
            [
                col
                for col in self.list_columns()
                if col not in ADFGlobalConfig.get_tech_cols()
            ]
        ]

    @abstractmethod
    def get_column(self, col_name: str) -> AbstractDataColumn:
        pass

    @abstractmethod
    def get_columns(self, cols: List[str]) -> "AbstractDataStructure":
        pass

    @abstractmethod
    def create_column_from_column(
        self, col_name: str, col_values: AbstractDataColumn
    ) -> None:
        pass

    @abstractmethod
    def create_column_from_value(self, col_name: str, value) -> None:
        pass

    @abstractmethod
    def set_column_from_column(
        self, col_name: str, col_values: AbstractDataColumn
    ) -> None:
        pass

    @abstractmethod
    def set_column_from_value(self, col_name: str, value) -> None:
        pass

    @abstractmethod
    def filter_from_column(self, col: AbstractDataColumn) -> "AbstractDataStructure":
        pass

    @abstractmethod
    def set_subset_from_column(
        self,
        col_name: str,
        col_filter: AbstractDataColumn,
        col_values: AbstractDataColumn,
    ):
        pass

    @abstractmethod
    def set_subset_from_value(
        self, col_name: str, col_filter: AbstractDataColumn, value: Any
    ):
        pass

    @abstractmethod
    def rename(self, names: Dict[str, str]) -> "AbstractDataStructure":
        pass

    def concretize(self, concrete_class: Type) -> Tuple[Any, Dict[str, Any]]:
        concretizer = self.concretizations[concrete_class]
        return concretizer.concretize(self), concretizer.extract_abstraction_params(
            self
        )

    @classmethod
    def abstract(
        cls, concrete_object: Any, abstraction_params: Dict[str, Any]
    ) -> "AbstractDataStructure":
        concretizer = cls.concretizations[type(concrete_object)]
        return concretizer.abstract(concrete_object, abstraction_params)

    @abstractmethod
    def to_list_of_dicts(self) -> List[Dict]:
        pass

    @classmethod
    @abstractmethod
    def from_list_of_dicts(
        cls, data: List[Dict[str, Any]], meta: Optional[Dict[str, str]]
    ) -> "AbstractDataStructure":
        pass

    # Data getting and setting

    def __getitem__(
        self, key: Union[str, AbstractDataColumn, List[str]]
    ) -> Union["AbstractDataStructure", AbstractDataColumn]:
        if isinstance(key, str):
            if not self.col_exists(key):
                raise ColumnNotFound(key, self)
            return self.get_column(key)
        elif isinstance(key, list):
            missing = [col for col in key if not self.col_exists(col)]
            if missing:
                raise ColumnNotFound(str(missing), self)
            return self.get_columns(key)
        elif isinstance(key, AbstractDataColumn):
            return self.filter_from_column(key)
        else:
            raise InputError(
                f'Cannot get item of type "{str(type(key))}" from {self.__class__.__name__}.'
            )

    def __setitem__(
        self,
        key: Union[str, Tuple[str, AbstractDataColumn]],
        value: Union[Any, AbstractDataColumn],
    ) -> None:
        if isinstance(key, str):
            if self.col_exists(key):
                if isinstance(value, AbstractDataColumn):
                    self.set_column_from_column(key, value)
                else:
                    self.set_column_from_value(key, value)
            else:
                if isinstance(value, AbstractDataColumn):
                    self.create_column_from_column(key, value)
                else:
                    self.create_column_from_value(key, value)
        elif isinstance(key, tuple):
            if (
                len(key) != 2
                or not isinstance(key[0], str)
                or not isinstance(key[1], AbstractDataColumn)
            ):
                raise InputError(
                    f"Require index tuple (str, AbstractDataColumn), not ({', '.join((type(e).__name__ for e in key))})."
                )
            if not self.col_exists(key[0]):
                raise InputError(f"Can only set subset for pre-existing columns.")
            if isinstance(value, AbstractDataColumn):
                self.set_subset_from_column(key[0], key[1], value)
            else:
                self.set_subset_from_value(key[0], key[1], value)
        else:
            raise InputError(
                f'Cannot set item of type "{str(type(key))}" in {self.__class__.__name__}.'
            )

    # ADS operations

    @abstractmethod
    def count(self) -> int:
        pass

    def __len__(self) -> int:
        return self.count()

    def __bool__(self) -> bool:
        return len(self) != 0

    def join(
        self,
        other: "AbstractDataStructure",
        left_on: List[str],
        right_on: List[str],
        how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
        l_modifier: Callable[[str], str] = lambda x: x,
        r_modifier: Callable[[str], str] = lambda x: x,
        modify_on: bool = True,
    ) -> "AbstractDataStructure":
        if len(left_on) != len(right_on):
            raise JoinError(
                f"# left keys = {len(left_on)}, # right keys = {len(right_on)}."
            )
        if len(left_on) != len(set(left_on)) or len(right_on) != len(set(left_on)):
            raise JoinError(
                f"Cannot repeat join keys (left : {str(left_on)}, right : {str(right_on)})."
            )
        if how not in ["left", "right", "outer", "inner", "cross"]:
            raise JoinError(f"Unknown join method '{how}'.")
        l_map = None
        r_map = None
        if how == "cross":
            if left_on:
                raise JoinError(
                    f"Cross join must have empty join keys (left : {str(left_on)}, right : {str(right_on)})."
                )
        else:
            if not left_on:
                raise JoinError(f"Non cross join must have non empty join keys.")
            l_map = {
                col: l_modifier(col) if (modify_on or col not in left_on) else col
                for col in self.list_columns()
            }
            r_map = {
                col: r_modifier(col) if (modify_on or col not in right_on) else col
                for col in other.list_columns()
            }
            acceptable_duplicates = [
                col for i, col in enumerate(left_on) if right_on[i] == col
            ]
            unacceptable_duplicates = [
                col
                for i, col in enumerate(l_map.values())
                if col in r_map.values() and col not in acceptable_duplicates
            ]
            if unacceptable_duplicates:
                raise JoinError(
                    f"Unacceptable duplicate output column names in join : {str(unacceptable_duplicates)}."
                )
        return self._join(
            other=other,
            left_on=left_on,
            right_on=right_on,
            how=how,
            l_map=l_map,
            r_map=r_map,
        )

    @abstractmethod
    def _join(
        self,
        other: "AbstractDataStructure",
        left_on: List[str],
        right_on: List[str],
        how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
        l_map: Dict[str, str] = None,
        r_map: Dict[str, str] = None,
    ) -> "AbstractDataStructure":
        pass

    def group_by(
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
    ) -> "AbstractDataStructure":
        missing_keys = [key for key in keys if key not in self.list_columns()]
        if missing_keys:
            raise GroupByError(
                f"Failed to group by on unknown keys {str(missing_keys)}."
            )
        duplicate_keys = [key for key in keys if key in outputs]
        if duplicate_keys:
            raise GroupByError(
                f"Cannot have a group by key be an output name : {str(duplicate_keys)}."
            )
        return self._group_by(keys=keys, outputs=outputs)

    @abstractmethod
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
    ) -> "AbstractDataStructure":
        pass

    def union(
        self, *others: "AbstractDataStructure", all: bool = True
    ) -> "AbstractDataStructure":
        cols = self.list_columns()
        for other in others:
            if set(cols) != set(other.list_columns()):
                raise UnionError(
                    f"Cannot perform union with mismatched columns : {str(cols)} VS {str(other.list_columns())}."
                )
        return self._union(*others, all=all)

    @abstractmethod
    def _union(
        self, *others: List["AbstractDataStructure"], all: bool
    ) -> "AbstractDataStructure":
        pass

    def distinct(self, keys: Optional[List[str]] = None) -> "AbstractDataStructure":
        keys = keys or self.list_columns()
        missing_keys = [key for key in keys if key not in self.list_columns()]
        if missing_keys:
            raise DistinctError(
                f"Failed to find distinct entries on unknown keys {str(missing_keys)}."
            )
        return self._distinct(keys)

    @abstractmethod
    def _distinct(self, keys: Optional[List[str]] = None) -> "AbstractDataStructure":
        pass

    def apply(
        self, output_column: str, func: Callable[[Dict], Any], cast: Type
    ) -> "AbstractDataStructure":
        return self._apply(output_column, func, cast)

    @abstractmethod
    def _apply(
        self, output_column: str, func: Callable[[Dict], Any], cast: Type
    ) -> "AbstractDataStructure":
        pass

    def sort(
        self, *cols: str, asc: Union[bool, List[bool]] = True
    ) -> "AbstractDataStructure":
        if asc is True:
            asc = [True for _ in cols]
        elif asc is False:
            asc = [False for _ in cols]
        if len(asc) != len(cols):
            raise ValueError(
                f"Ascending option must be equal to sort columns, got : {len(cols)} sort columns, {len(asc)} ascending options"
            )
        return self._sort(list(cols), asc)

    @abstractmethod
    def _sort(self, cols: List[str], asc: List[bool]) -> "AbstractDataStructure":
        pass

    def limit(self, n: int) -> "AbstractDataStructure":
        try:
            n = int(n)
        except Exception as e:
            raise ValueError(
                f"Failed to cast input '{str(n)}' to limit method to int, got : {e.__class__.__name__} : {str(e)}"
            )
        return self._limit(n)

    @abstractmethod
    def _limit(self, n: int) -> "AbstractDataStructure":
        pass

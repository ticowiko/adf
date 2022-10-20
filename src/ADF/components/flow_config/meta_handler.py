import logging
import datetime

from typing import Dict, Union, Type, Optional, List

from ADF.config import ADFGlobalConfig
from ADF.exceptions import (
    MetaCheckFailure,
    InvalidMeta,
    InvalidMetaColumn,
)
from ADF.components.data_structures import AbstractDataStructure
from ADF.utils import ToDictMixin, extract_parameter


type_map = {
    "str": str,
    "int": int,
    "float": float,
    "complex": complex,
    "bool": bool,
    "datetime": datetime.datetime,
    "date": datetime.date,
    "timedelta": datetime.timedelta,
    None: None,
}
inverted_type_map = {val: key for key, val in type_map.items()}


class ADFMetaColumn(ToDictMixin):
    flat_attrs = ["name", "cast", "on_missing", "fill_value", "in_partition"]

    handled_types = [
        str,
        int,
        float,
        complex,
        bool,
        datetime.datetime,
        datetime.date,
        datetime.timedelta,
        None,
    ]
    missing_col_handled_behaviors = [
        "ignore",
        "fail",
        "fill",
    ]

    def __init__(
        self,
        name: str,
        cast: Optional[
            Union[
                Type[str],
                Type[int],
                Type[float],
                Type[complex],
                Type[bool],
                Type[datetime.datetime],
                Type[datetime.date],
                Type[datetime.timedelta],
            ]
        ] = None,
        on_missing: str = "ignore",
        fill_value: Optional = None,
        in_partition: bool = False,
    ):
        self.name = name
        self.cast = cast
        self.on_missing = on_missing
        self.fill_value = fill_value
        self.in_partition = in_partition

    def validate(self):
        errs = []
        if self.cast not in self.handled_types:
            errs.append(
                f"Column '{self.name}' has unsupported cast type '{self.cast}'."
            )
        if self.on_missing not in self.missing_col_handled_behaviors:
            errs.append(
                f"Column '{self.name}' has unsupported missing column behavior '{self.on_missing}'."
            )
        if self.fill_value is not None and self.on_missing != "fill":
            errs.append(
                f"Column '{self.name}' has fill value despite 'on_missing' being set to '{self.fill_value}'."
            )
        if errs:
            raise InvalidMetaColumn(errs)

    def handle_missing(self, ads: AbstractDataStructure) -> AbstractDataStructure:
        logging.info(
            f"Handling missing column '{self.name}' with option '{self.on_missing}'..."
        )
        if self.on_missing == "ignore":
            return ads
        elif self.on_missing == "fail":
            raise MetaCheckFailure(f"Unauthorized missing columns {str(self.name)}")
        elif self.on_missing == "fill":
            ads[self.name] = self.fill_value
            return ads
        else:
            raise MetaCheckFailure(
                f"Unknown missing columns behavior at runtime : '{self.on_missing}'"
            )

    def handle_matching(self, ads: AbstractDataStructure) -> AbstractDataStructure:
        logging.info(
            f"Handling matched column '{self.name}' with cast '{self.cast.__name__}'..."
        )
        if self.cast is not None:
            ads[self.name] = ads[self.name].as_type(self.cast)
        return ads

    def get_dict_cast(self):
        return inverted_type_map[self.cast]


class ADFMetaHandler(ToDictMixin):
    flat_attrs = ["on_extra"]
    iter_attrs = ["columns"]

    extra_col_handled_behaviors = [
        "ignore",
        "fail",
        "cut",
    ]

    def __init__(
        self,
        columns: Optional[List[ADFMetaColumn]] = None,
        on_extra: str = "ignore",
    ):
        self.columns = columns or []
        self.on_extra = on_extra

    def get_column_dict(self) -> Dict[str, ADFMetaColumn]:
        return {column.name: column for column in self.columns}

    @classmethod
    def from_config(cls, config: Dict) -> "ADFMetaHandler":
        if not isinstance(config, dict):
            raise ValueError(
                f"Meta handler config must be a dict, not '{config.__class__.__name__}'"
            )
        return cls(
            columns=[
                ADFMetaColumn(
                    name=extract_parameter(col_config, "name", "meta column config"),
                    cast=type_map[col_config.get("cast", None)],
                    on_missing=col_config.get(
                        "on_missing", config.get("on_missing_default", "ignore")
                    ),
                    fill_value=col_config.get("fill_value", None),
                    in_partition=col_config.get("in_partition", False),
                )
                for col_config in config.get("columns", [])
            ],
            on_extra=config.get("on_extra", "ignore"),
        )

    def validate(self):
        errs = []
        seen = set()
        for col in self.columns:
            try:
                col.validate()
            except InvalidMetaColumn as e:
                errs += e.errs
            if col.name in seen:
                errs += f"Multiple columns with name {col.name} in meta."
            seen.add(col.name)
        if self.on_extra not in self.extra_col_handled_behaviors:
            errs.append(f"Unsupported extra column behavior '{self.on_extra}'.")
        if errs:
            raise InvalidMeta(errs)

    def format(
        self, ads: AbstractDataStructure, batch_id: str, timestamp: datetime.datetime
    ):
        meta_cols = self.get_column_dict()
        ads_cols = ads.list_columns()
        matching = [col for col in set(ads_cols) & set(meta_cols.keys())]
        extra = [col for col in ads_cols if col not in meta_cols]
        missing = [col for col in meta_cols if col not in ads_cols]
        logging.info(f"ADS columns     : {', '.join(ads_cols)}")
        logging.info(f"Meta columns    : {', '.join(meta_cols.keys())}")
        logging.info(f"Extra columns   : {', '.join(extra)}")
        logging.info(f"Missing columns : {', '.join(missing)}")

        # Handle extra columns
        if extra:
            logging.info(
                f"Handling extra columns : {', '.join(extra)} with option '{self.on_extra}'..."
            )
        else:
            logging.info(f"No extra columns (option '{self.on_extra}')")
        if self.on_extra == "ignore":
            pass
        elif self.on_extra == "fail":
            if extra:
                raise MetaCheckFailure(f"Unauthorized extra columns {str(extra)}")
        elif self.on_extra == "cut":
            ads = ads[matching]
        else:
            raise MetaCheckFailure(
                f"Unknown extra columns behavior at runtime : '{str(self.on_extra)}'"
            )

        # Handle missing columns
        for col in missing:
            ads = meta_cols[col].handle_missing(ads)

        # Handle matching columns
        for col in matching:
            ads = meta_cols[col].handle_matching(ads)

        # Add tech cols
        ads[ADFGlobalConfig.BATCH_ID_COLUMN_NAME] = batch_id
        ads[ADFGlobalConfig.TIMESTAMP_COLUMN_NAME] = timestamp

        # Return meta formatted object
        return ads

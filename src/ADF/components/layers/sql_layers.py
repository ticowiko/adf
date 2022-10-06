import os
import logging

from abc import ABC, abstractmethod
from typing import List, Optional, Dict
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, DeclarativeMeta, Session
from sqlalchemy.engine import Connection

from ADF.exceptions import UnhandledMeta
from ADF.components.layers import AbstractDataLayer
from ADF.components.data_structures import SQLDataStructure
from ADF.components.flow_config import ADFStep, ADFLandingStep
from ADF.utils import create_table_meta
from ADF.config import ADFGlobalConfig


class SQLDataLayer(AbstractDataLayer, ABC):
    def __init__(
        self,
        as_layer: str,
        engine: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[str] = None,
        user: Optional[str] = None,
        pw: Optional[str] = None,
        table_prefix: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(as_layer)
        self._session = None
        self._conn = None
        self.engine = engine
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.pw = pw
        self.session_kwargs = kwargs
        self.table_metas: Dict[ADFStep, DeclarativeMeta] = {}
        self.base = declarative_base()
        self.table_prefix = table_prefix or ""

    @property
    def url(self) -> Optional[str]:
        if not self.engine or not self.host:
            return None
        if self.pw and not self.user:
            raise ValueError(
                f"{str(self)} cannot create connection string when a password is given with no user"
            )
        credential_string = (
            f"{self.user}:{self.pw}@"
            if (self.user and self.pw)
            else (f"{self.user}@" if self.user else "")
        )
        port_string = f":{self.port}" if self.port else ""
        db_string = f"/{self.db}" if self.db else ""
        return f"{self.engine}://{credential_string}{self.host}{port_string}{db_string}"

    @property
    def session(self) -> Session:
        if self._session is None:
            if self.url is None:
                raise ValueError(
                    f"Cannot create session from empty url in {self}. Are you sure you've setup this layer ?"
                )
            self._session = sessionmaker(
                bind=create_engine(self.url, **self.session_kwargs)
            )()
        return self._session

    @property
    def conn(self) -> Connection:
        if self._conn is None:
            if self.url is None:
                raise ValueError(
                    f"Cannot create connection from empty url in {self}. Are you sure you've setup this layer ?"
                )
            self._conn = self.session.get_bind().connect()
            self._conn.execution_options(isolation_level="AUTOCOMMIT")
        return self._conn

    def validate_step(self, step: ADFStep):
        acceptable_extra = ["fail", "cut"]
        acceptable_missing = ["ignore", "fail", "fill"]
        if step.meta.on_extra not in acceptable_extra or any(
            col.on_missing
            for col in step.meta.columns
            if col.on_missing not in acceptable_missing
        ):
            raise UnhandledMeta(self, step, acceptable_extra, acceptable_missing)

    def get_table_name(self, step: ADFStep) -> str:
        return f"{self.table_prefix}{step.flow.collection.name}_{step.flow.name}_{step.name}_{step.version}"

    def get_partition_name(self, step: ADFStep, vals: List) -> str:
        return f"{self.get_table_name(step)}_partition_{'_'.join([str(val) for val in vals])}"

    @abstractmethod
    def attach_partition(self, ads: SQLDataStructure, step: ADFStep):
        pass

    @abstractmethod
    def get_step_table_creation_kwargs(self, step: ADFStep) -> Dict:
        pass

    @staticmethod
    def add_partition_to_pk() -> bool:
        return False

    def get_step_table_meta(
        self, step: ADFStep, extra_cols: Optional[List[str]] = None
    ) -> DeclarativeMeta:
        extra_cols = extra_cols or []
        if step not in self.table_metas:
            self.table_metas[step] = create_table_meta(
                base=self.base,
                name=self.get_table_name(step),
                cols={
                    **{col.name: col.cast for col in step.meta.columns},
                    **{col: str for col in extra_cols},
                },
                primary_key=step.get_partition_key()
                if self.add_partition_to_pk()
                else [],
                include_timestamp_col=not isinstance(step, ADFLandingStep),
                **self.get_step_table_creation_kwargs(step),
            )
        return self.table_metas[step]

    def setup_steps(self, steps: List[ADFStep]) -> None:
        for step in steps:
            self.validate_step(step)
            self.get_step_table_meta(step)
        self.base.metadata.create_all(self.session.get_bind())

    def get_step_tech_cols(self, step: ADFStep) -> List[str]:
        return (
            [
                ADFGlobalConfig.BATCH_ID_COLUMN_NAME,
            ]
            if isinstance(step, ADFLandingStep)
            else [
                ADFGlobalConfig.BATCH_ID_COLUMN_NAME,
                ADFGlobalConfig.TIMESTAMP_COLUMN_NAME,
            ]
        )

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SQLDataStructure:
        self.validate_step(step)
        table = self.get_step_table_meta(step)
        subquery = (
            self.session.query(table)
            .filter(getattr(table, ADFGlobalConfig.BATCH_ID_COLUMN_NAME) == batch_id)
            .subquery()
        )
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={
                col_name: subquery.c[col_name]
                for col_name in [col.name for col in step.meta.columns]
                + self.get_step_tech_cols(step)
            },
        )

    def read_full_data(self, step: ADFStep) -> SQLDataStructure:
        table = self.get_step_table_meta(step)
        subquery = self.session.query(table).subquery()
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={
                col_name: subquery.c[col_name]
                for col_name in [col.name for col in step.meta.columns]
                + [ADFGlobalConfig.BATCH_ID_COLUMN_NAME]
                + self.get_step_tech_cols(step)
            },
        )

    def read_batches_data(
        self, step: ADFStep, batch_ids: List[str]
    ) -> Optional[SQLDataStructure]:
        if not batch_ids:
            return None
        self.validate_step(step)
        table = self.get_step_table_meta(step)
        subquery = (
            self.session.query(table)
            .filter(getattr(table, ADFGlobalConfig.BATCH_ID_COLUMN_NAME).in_(batch_ids))
            .subquery()
        )
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={
                col_name: subquery.c[col_name]
                for col_name in [col.name for col in step.meta.columns]
                + [
                    ADFGlobalConfig.BATCH_ID_COLUMN_NAME,
                    ADFGlobalConfig.TIMESTAMP_COLUMN_NAME,
                ]
            },
        )

    def write_batch_data(
        self, ads: SQLDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        if step.get_partition_key():
            self.attach_partition(ads, step)
        cols = ads.list_columns()
        self.session.get_bind().execute(
            self.get_step_table_meta(step)
            .__table__.insert()
            .from_select(names=cols, select=ads.query(cols))
        )
        self.session.commit()

    def detect_batches(self, step: ADFStep) -> List[str]:
        return [
            e[ADFGlobalConfig.BATCH_ID_COLUMN_NAME]
            for e in self.read_full_data(step)
            .distinct([ADFGlobalConfig.BATCH_ID_COLUMN_NAME])
            .to_list_of_dicts()
        ]

    def delete_step(self, step: ADFStep) -> None:
        meta = self.get_step_table_meta(step)
        self.session.query(meta).delete()
        self.session.commit()

    def delete_batch(self, step: ADFStep, batch_id: str) -> None:
        meta = self.get_step_table_meta(step)
        self.session.query(meta).filter(
            getattr(meta, ADFGlobalConfig.BATCH_ID_COLUMN_NAME) == batch_id
        ).delete()
        self.session.commit()


class SQLiteDataLayer(SQLDataLayer):
    def __init__(self, as_layer: str, db_path: str, table_prefix: Optional[str] = None):
        super().__init__(
            as_layer=as_layer,
            engine="sqlite",
            host=f"/{db_path}",
            table_prefix=table_prefix,
            echo=False,
        )
        self.db_path = db_path

    def get_step_table_creation_kwargs(self, step: ADFStep) -> Dict:
        return {}

    def attach_partition(self, ads: SQLDataStructure, step: ADFStep):
        logging.warning(
            f"WARNING : {self.__class__.__name__} does not support partitioning, ignoring for step {step}."
        )

    def setup_layer(self) -> None:
        Path(self.db_path).touch()

    def destroy(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)


class PostgreSQLDataLayer(SQLDataLayer):
    def __init__(
        self,
        as_layer: str,
        engine: Optional[str] = "postgresql",
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[str] = None,
        user: Optional[str] = None,
        pw: Optional[str] = None,
        table_prefix: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            as_layer=as_layer,
            engine=engine,
            host=host,
            port=port,
            db=db,
            user=user,
            pw=pw,
            table_prefix=table_prefix,
            **kwargs,
        )

    @staticmethod
    def get_partition_col(step: ADFStep) -> str:
        partition_key = step.get_partition_key()
        if len(partition_key) > 1:
            raise ValueError(
                "PostgresQL can only handle single column LIST partitioning."
            )
        return partition_key[0] if partition_key else None

    @staticmethod
    def add_partition_to_pk() -> bool:
        return True

    def get_step_table_creation_kwargs(self, step: ADFStep) -> Dict:
        partition_col = self.get_partition_col(step)
        return (
            {"__table_args__": {"postgresql_partition_by": f"LIST ({partition_col})"}}
            if partition_col
            else {}
        )

    def setup_layer(self) -> None:
        pass

    def list_tables(self) -> List[str]:
        return [
            result[0]
            for result in self.conn.execute(
                f"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' AND tablename LIKE '{self.table_prefix}%%';"
            )
        ]

    def destroy(self) -> None:
        tables = self.list_tables()
        for table in tables:
            self.conn.execute(f'DROP TABLE IF EXISTS "{table}";')

    def attach_partition(self, ads: SQLDataStructure, step: ADFStep):
        partition_col = self.get_partition_col(step)
        if partition_col is not None:
            partitions = [
                e[partition_col]
                for e in ads.distinct([partition_col]).to_list_of_dicts()
            ]
            tables = self.list_tables()
            for partition in partitions:
                partition_table = self.get_partition_name(step, [partition])
                if partition_table not in tables:
                    create_table_meta(
                        base=self.base,
                        name=partition_table,
                        cols={col.name: col.cast for col in step.meta.columns},
                        primary_key=step.get_partition_key(),
                    ).__table__.create(bind=self.session.get_bind())
                    self.conn.execute(
                        f'ALTER TABLE "{self.get_table_name(step)}" ATTACH PARTITION "{partition_table}" FOR VALUES IN (\'{partition}\')'
                    )

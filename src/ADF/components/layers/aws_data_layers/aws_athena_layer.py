import logging
import time
import datetime
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote_plus

from boto3 import Session as Boto3Session

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, DeclarativeMeta, Session
from sqlalchemy.engine import Engine

from ADF.config import ADFGlobalConfig
from ADF.utils import (
    athena_client,
    create_table_meta,
    s3_delete_prefix,
    s3_list_folders,
)
from ADF.exceptions import UnhandledMeta
from ADF.components.data_structures import SQLDataStructure
from ADF.components.flow_config import ADFStep, ADFLandingStep
from ADF.components.layers import AbstractDataLayer


class AWSAthenaLayer(AbstractDataLayer):
    athena_type_map = {
        None: "STRING",
        str: "STRING",
        int: "INT",
        float: "DOUBLE",
        # complex: None,
        bool: "BOOLEAN",
        datetime.datetime: "TIMESTAMP",
        datetime.date: "DATE",
        # datetime.timedelta: None,
    }

    def __init__(
        self,
        as_layer: str,
        db_name: str,
        table_prefix: str,
        bucket: str,
        s3_prefix: str,
        landing_format: str = "csv",
        separator: str = ",",
    ):
        super().__init__(as_layer)
        self.db_name = db_name
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        self.table_prefix = table_prefix
        self.landing_format = landing_format
        self.separator = separator

        self.table_metas: Dict[ADFStep, DeclarativeMeta] = {}
        self.base = declarative_base()
        self._url = None
        self._engine = None
        self._session = None

    @property
    def url(self) -> str:
        if self._url is None:
            credentials = Boto3Session().get_credentials().get_frozen_credentials()
            self._url = f"awsathena+rest://{quote_plus(credentials.access_key)}:{quote_plus(credentials.secret_key)}@athena.{ADFGlobalConfig.AWS_REGION}.amazonaws.com:443/{self.db_name}?s3_staging_dir={quote_plus(f's3://{self.bucket}/{self.s3_prefix}staging/')}"
        return self._url

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self.url)
        return self._engine

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = sessionmaker(bind=self.engine)()
        return self._session

    def validate_step(self, step: ADFStep):
        acceptable_extra = ["fail", "cut"]
        acceptable_missing = ["ignore", "fail", "fill"]
        if step.meta.on_extra not in acceptable_extra or any(
            col.on_missing
            for col in step.meta.columns
            if col.on_missing not in acceptable_missing
        ):
            raise UnhandledMeta(self, step, acceptable_extra, acceptable_missing)

    @staticmethod
    def await_query_execution(
        retry_rate: int = 5,
        n_retries: Optional[int] = None,
        fail_on_failure: bool = True,
        log_tag: Optional[str] = None,
        **kwargs,
    ) -> Tuple[bool, Dict]:
        if log_tag:
            logging.info((len(log_tag) + 4) * "*")
            logging.info(f"* {log_tag} *")
            logging.info(kwargs.get("QueryString", ""))
            logging.info((len(log_tag) + 4) * "*")
        query_id = athena_client.start_query_execution(**kwargs)["QueryExecutionId"]
        logging.info(f"Sent ATHENA query '{query_id}'...")
        n_retry = -1
        while (n_retries is None) or (n_retry < n_retries):
            n_retry += 1
            response = athena_client.get_query_execution(QueryExecutionId=query_id)
            state = response["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                return True, response
            elif state in ["FAILED", "CANCELLED"]:
                if fail_on_failure:
                    raise RuntimeError(
                        f"Query '{query_id}' failed with state '{state}', reason : {response['QueryExecution']['Status']['StateChangeReason']}"
                    )
                else:
                    return False, response
            logging.info(
                f"Waiting on ATHENA query '{query_id}' with state '{state}'..."
            )
            time.sleep(retry_rate)
        logging.info(f"Timeout awaiting query '{query_id}' execution !")
        raise RuntimeError(f"Timeout awaiting query '{query_id}' execution !")

    def get_table_name(self, step: ADFStep) -> str:
        return f"{self.table_prefix}{step.flow.collection.name}_{step.flow.name}_{step.name}_{step.version}"

    def get_step_s3_suffix(self, step: ADFStep) -> str:
        return (
            f"{step.flow.collection.name}/{step.flow.name}/{step.name}/{step.version}/"
        )

    def get_step_s3_prefix(self, step: ADFStep) -> str:
        return f"{self.s3_prefix}{self.get_step_s3_suffix(step)}"

    def repair_partitions(self, step: ADFStep):
        self.await_query_execution(
            retry_rate=1,
            n_retries=120,
            fail_on_failure=True,
            log_tag=f"ATHENA PARTITION REPAIR {step}",
            QueryString=f"MSCK REPAIR TABLE `{self.get_table_name(step)}`",
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket}/{self.s3_prefix}step_batch_partition_repair/{self.get_step_s3_suffix(step)}",
            },
            QueryExecutionContext={
                "Database": self.db_name,
            },
        )

    def create_external_table(
        self,
        step: ADFStep,
        bucket: str,
        s3_prefix: str,
        step_format: str,
        partition_cols: List[str],
    ) -> None:
        self.validate_step(step)
        self.await_query_execution(
            retry_rate=1,
            n_retries=60,
            log_tag=f"ATHENA DROP {step}",
            QueryString=f"DROP TABLE IF EXISTS `{self.get_table_name(step)}`",
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket}/{self.s3_prefix}step_pre_setup_drop/{self.get_step_s3_suffix(step)}",
            },
            QueryExecutionContext={
                "Database": self.db_name,
            },
        )
        meta_dict = step.meta.get_column_dict()
        partition_string = ",\n".join(
            f"  `{col}` {self.athena_type_map[meta_dict[col].cast] if (col in meta_dict) else 'STRING'}"
            for col in partition_cols
        )
        if step_format == "csv":
            storage_string = (
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'\n"
                + f"WITH SERDEPROPERTIES ('separatorChar'='{self.separator}', 'quoteChar'='\"')\n"
                + "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'"
                + "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
            )
        elif step_format == "parquet":
            storage_string = f"STORED AS PARQUET"
        else:
            raise ValueError(f"Unhandled storage format {step_format} in {str(self)} !")
        query_string = "\n".join(
            [
                f"CREATE EXTERNAL TABLE `{self.get_table_name(step)}` (",
                f"{ADFGlobalConfig.TIMESTAMP_COLUMN_NAME} STRING,"
                if not isinstance(step, ADFLandingStep)
                else "",
                ",\n".join(
                    f"  `{col.name}` {'STRING' if step_format == 'csv' else self.athena_type_map[col.cast]}"
                    for col in step.meta.columns
                    if col not in partition_cols
                ),
                ") PARTITIONED BY (",
                partition_string,
                f") {storage_string}",
                f"LOCATION 's3://{bucket}/{s3_prefix}'",
                "TBLPROPERTIES ('skip.header.line.count'='1')"
                if step_format == "csv"
                else "",
                "TBLPROPERTIES ('parquet.compression'='gzip')"
                if step_format == "parquet"
                else "",
            ]
        )
        self.await_query_execution(
            retry_rate=1,
            n_retries=60,
            log_tag=f"ATHENA CREATE {step}",
            QueryString=query_string,
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket}/{self.s3_prefix}step_write_out_setup/{self.get_step_s3_suffix(step)}",
            },
            QueryExecutionContext={
                "Database": self.db_name,
            },
        )
        self.repair_partitions(step)

    def setup_layer(self) -> None:
        success, response = self.await_query_execution(
            retry_rate=5,
            n_retries=12,
            fail_on_failure=False,
            QueryString=f"CREATE DATABASE {self.db_name}",
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket}/{self.s3_prefix}db_creation_query/",
            },
        )
        if not success:
            error = response["QueryExecution"]["Status"]["StateChangeReason"]
            if not error.endswith(f"Database {self.db_name} already exists"):
                raise RuntimeError(
                    f"Unexpected error during db deletion in {str(self)} : {error}"
                )
            logging.info(
                f"In {str(self)} : Ignoring DB {self.db_name} creation failure as DB already exists..."
            )

    def get_step_table_meta(self, step: ADFStep) -> DeclarativeMeta:
        self.validate_step(step)
        if step not in self.table_metas:
            self.table_metas[step] = create_table_meta(
                base=self.base,
                name=self.get_table_name(step),
                cols={col.name: col.cast for col in step.meta.columns},
                primary_key=step.meta.columns[0].name,
                include_serial_pk=False,
                include_timestamp_col=not isinstance(step, ADFLandingStep),
            )
        return self.table_metas[step]

    def get_step_format(self, step: ADFStep):
        return (
            self.landing_format.lower()
            if isinstance(step, ADFLandingStep)
            else "parquet"
        )

    def setup_steps(self, steps: List[ADFStep]) -> None:
        for step in steps:
            logging.info(f"SETTING UP STEP {step}")
            self.validate_step(step)
            self.create_external_table(
                step,
                self.bucket,
                self.get_step_s3_prefix(step),
                self.get_step_format(step),
                [ADFGlobalConfig.BATCH_ID_COLUMN_NAME, *step.get_partition_key()],
            )

    def detect_batches(self, step: ADFStep) -> List[str]:
        step_prefix = self.get_step_s3_prefix(step)
        return [
            partition[partition.find("=") + 1 : -1]
            for partition in [
                prefix[len(step_prefix) :]
                for prefix in s3_list_folders(self.bucket, step_prefix)
            ]
            if partition.startswith(f"{ADFGlobalConfig.BATCH_ID_COLUMN_NAME}=")
        ]

    def destroy(self) -> None:
        success, response = self.await_query_execution(
            retry_rate=5,
            n_retries=12,
            fail_on_failure=False,
            log_tag=f"ATHENA DROP DB {self.db_name}",
            QueryString=f"DROP DATABASE {self.db_name} CASCADE",
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket}/{self.s3_prefix}db_deletion_query/",
            },
        )
        if not success:
            error = response["QueryExecution"]["Status"]["StateChangeReason"]
            if not error.endswith(f"Database does not exist: {self.db_name}"):
                raise RuntimeError(
                    f"Unexpected error during db deletion in {str(self)} : {error}"
                )
            logging.info(
                f"In {str(self)} : Ignoring DB {self.db_name} deletion failure as DB doesn't exist..."
            )

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SQLDataStructure:
        self.repair_partitions(step)
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
                + [
                    ADFGlobalConfig.BATCH_ID_COLUMN_NAME,
                    *(
                        [ADFGlobalConfig.TIMESTAMP_COLUMN_NAME]
                        if not isinstance(step, ADFLandingStep)
                        else []
                    ),
                ]
            },
        )

    def read_full_data(self, step: ADFStep) -> SQLDataStructure:
        self.repair_partitions(step)
        table = self.get_step_table_meta(step)
        subquery = self.session.query(table).subquery()
        return SQLDataStructure(
            session=self.session,
            subquery=subquery,
            cols={
                col_name: subquery.c[col_name]
                for col_name in [col.name for col in step.meta.columns]
                + [
                    ADFGlobalConfig.BATCH_ID_COLUMN_NAME,
                    *(
                        [ADFGlobalConfig.TIMESTAMP_COLUMN_NAME]
                        if not isinstance(step, ADFLandingStep)
                        else []
                    ),
                ]
            },
        )

    def get_step_columns(self, step: ADFStep) -> List[str]:
        partition_key = step.get_partition_key()
        return [
            *(
                [ADFGlobalConfig.TIMESTAMP_COLUMN_NAME]
                if not isinstance(step, ADFLandingStep)
                else []
            ),
            *[
                column.name
                for column in step.meta.columns
                if column.name not in partition_key
            ],
            ADFGlobalConfig.BATCH_ID_COLUMN_NAME,
            *partition_key,
        ]

    def insert(self, ads: SQLDataStructure, step: ADFStep, batch_id: str) -> None:
        self.await_query_execution(
            retry_rate=5,
            n_retries=12,
            log_tag=f"ATHENA INSERT {step}::{batch_id}",
            QueryString=f'INSERT INTO "{self.get_table_name(step)}"\n{ads.query_string(self.get_step_columns(step))}',
            ResultConfiguration={
                "OutputLocation": f"s3://{self.bucket}/{self.s3_prefix}inserts/{self.get_step_s3_suffix(step)}batch_id={batch_id}/",
            },
            QueryExecutionContext={
                "Database": self.db_name,
            },
        )

    def write_batch_data(
        self, ads: SQLDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.insert(ads, step, batch_id)
        self.repair_partitions(step)

    def delete_step(self, step: ADFStep) -> None:
        s3_delete_prefix(
            self.bucket,
            f"{self.get_step_s3_prefix(step)}",
            ignore_nosuchbucket=True,
        )
        self.repair_partitions(step)

    def delete_batch(self, step: ADFStep, batch_id: str) -> None:
        s3_delete_prefix(
            self.bucket,
            f"{self.get_step_s3_prefix(step)}{ADFGlobalConfig.BATCH_ID_COLUMN_NAME}={batch_id}/",
            ignore_nosuchbucket=True,
        )
        self.repair_partitions(step)

    def output_prebuilt_config(self) -> Dict[str, str]:
        return {
            "db_name": self.db_name,
            "bucket": self.bucket,
            "s3_prefix": self.s3_prefix,
            "table_prefix": self.table_prefix,
            "landing_format": self.landing_format,
            "separator": self.separator,
        }

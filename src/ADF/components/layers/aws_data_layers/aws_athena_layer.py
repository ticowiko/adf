import logging
import time
import datetime
from typing import List, Dict, Tuple, Optional

from ADF.config import ADFGlobalConfig
from ADF.utils import athena_client
from ADF.components.data_structures import AbstractDataStructure
from ADF.components.flow_config import ADFStep, ADFStartingStep
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
    ):
        super().__init__(as_layer)
        self.db_name = db_name
        self.bucket = bucket
        self.s3_prefix = s3_prefix
        self.table_prefix = table_prefix
        self.landing_format = landing_format

    @staticmethod
    def await_query_execution(
        retry_rate: int = 5, n_retries: Optional[int] = None, fail_on_failure: bool = True, log_tag: Optional[str] = None, **kwargs
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
            logging.info(f"Waiting on ATHENA query '{query_id}' with state '{state}'...")
            time.sleep(retry_rate)
        logging.info(f"Timeout awaiting query '{query_id}' execution !")
        raise RuntimeError(f"Timeout awaiting query '{query_id}' execution !")

    def get_table_name(self, step: ADFStep) -> str:
        return f"{self.table_prefix}{step.flow.collection.name}_{step.flow.name}_{step.name}_{step.version}"

    def get_step_s3_suffix(self, step: ADFStep) -> str:
        return f"{step.flow.collection.name}/{step.flow.name}/{step.name}/{step.version}/"

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

    def create_external_table(self, step: ADFStep, bucket: str, s3_prefix: str, step_format: str, partition_cols: List[str]) -> None:
        # TODO : check strict meta
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
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
            )
        elif step_format == "parquet":
            storage_string = f"STORED AS parquet"
        else:
            raise ValueError(
                f"Unhandled storage format {step_format} in {str(self)} !"
            )
        query_string = "\n".join(
            [
                f"CREATE EXTERNAL TABLE `{self.get_table_name(step)}` (",
                f"{ADFGlobalConfig.TIMESTAMP_COLUMN_NAME} STRING,",
                ",\n".join(
                    f"  `{col.name}` {self.athena_type_map[col.cast]}"
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

    def setup_steps(self, steps: List[ADFStep]) -> None:
        for step in steps:
            if isinstance(step, ADFStartingStep):
                self.create_external_table(
                    step=step,
                    bucket=self.bucket,
                    s3_prefix=f"{self.s3_prefix}{self.get_step_s3_suffix(step)}",
                    step_format=self.landing_format,
                    partition_cols=[],
                )
            logging.warning(
                f"Internal steps {','.join([str(step) for step in steps])} ignored in {str(self)} on setup..."
            )

    def detect_batches(self, step: ADFStep) -> List[str]:
        raise NotImplementedError(
            f"Batch detection for step {str(step)} currently unsupported in {str(self)} !"
        )

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

    def read_batch_data(self, step: ADFStep, batch_id: str) -> AbstractDataStructure:
        raise NotImplementedError(
            f"Reading internal batch {str(step)}::{batch_id} currently unsupported in {str(self)} !"
        )

    def read_full_data(self, step: ADFStep) -> AbstractDataStructure:
        raise NotImplementedError(
            f"Reading full data for step {str(step)} currently unsupported in {str(self)} !"
        )

    def write_batch_data(
        self, ads: AbstractDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        raise NotImplementedError(
            f"Writing batch data {str(step)}::{batch_id} currently unsupported in {str(self)} !"
        )

    def delete_step(self, step: ADFStep) -> None:
        raise NotImplementedError(
            f"Deleting step {str(step)} currently unsupported in {str(self)} !"
        )

    def delete_batch(self, step: ADFStep, batch_id: str) -> None:
        raise NotImplementedError(
            f"Deleting batch {str(step)}::{batch_id} currently unsupported in {str(self)} !"
        )

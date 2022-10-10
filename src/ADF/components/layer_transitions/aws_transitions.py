import os
import logging
import datetime

from typing import List, Type, Optional

from sqlalchemy.exc import ProgrammingError

from ADF.components.data_structures import (
    SQLDataStructure,
    PandasDataStructure,
    SparkDataStructure,
    AbstractDataStructure,
)
from ADF.components.flow_config import ADFStep
from ADF.components.layer_transitions import ADLTransition
from ADF.components.layers import (
    AbstractDataLayer,
    AWSLambdaLayer,
    AWSBaseEMRLayer,
    AWSRedshiftLayer,
    AWSAthenaLayer,
)
from ADF.exceptions import ADLUnsupportedTransition
from ADF.utils import MetaCSVToPandas, s3_list_objects
from ADF.config import ADFGlobalConfig


class LambdaToEMRTransition(ADLTransition):
    def default_to_write_out(self) -> bool:
        return False

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [AWSLambdaLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [AWSBaseEMRLayer]

    def write_batch_data(
        self, ads: PandasDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        raise ADLUnsupportedTransition(self, "Write out transition not supported.")

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SparkDataStructure:
        self.layer_in: AWSLambdaLayer
        self.layer_out: AWSBaseEMRLayer
        ads = SparkDataStructure(
            df=self.layer_out.spark.read.format("csv")
            .options(header="true", inferSchema="true")
            .load(
                f"s3a://{self.layer_in.bucket}/{self.layer_in.batch_to_key(step, batch_id)}"
            )
        )
        return ads.rename(MetaCSVToPandas.cols_to_meta(ads.list_columns())[0])

    def read_full_data(self, step: ADFStep) -> SparkDataStructure:
        self.layer_in: AWSLambdaLayer
        self.layer_out: AWSBaseEMRLayer
        ads = SparkDataStructure(
            df=self.layer_out.spark.createDataFrame(
                self.layer_in.read_full_data(step).df
            )
        )
        return ads.rename(MetaCSVToPandas.cols_to_meta(ads.list_columns())[0])

    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        raise ADLUnsupportedTransition(self, "Write out transition not supported.")

    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def delete_step_write_out(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_step_read_in(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)

    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)


class EMRToEMRTransition(ADLTransition):
    def default_to_write_out(self) -> bool:
        return True

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [AWSBaseEMRLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [AWSBaseEMRLayer]

    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SparkDataStructure:
        self.layer_in: AWSBaseEMRLayer
        return self.layer_in.read_batch_data(step, batch_id)

    def read_full_data(self, step: ADFStep) -> SparkDataStructure:
        self.layer_in: AWSBaseEMRLayer
        return self.layer_in.read_full_data(step)

    def write_batch_data(
        self, ads: SparkDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.layer_out: AWSBaseEMRLayer
        self.layer_out.write_batch_data(ads, step, batch_id)

    def delete_step_write_out(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_step_read_in(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)

    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)


class EMRToRedshiftTransition(ADLTransition):
    redshift_type_map = {
        None: "VARCHAR(MAX)",
        str: "VARCHAR(MAX)",
        int: "INTEGER",
        float: "FLOAT",
        # complex: None,
        bool: "BOOLEAN",
        datetime.datetime: "TIMESTAMPTZ",
        datetime.date: "DATE",
        # datetime.timedelta: None,
    }

    def default_to_write_out(self) -> bool:
        return True

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [AWSBaseEMRLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [AWSRedshiftLayer]

    def execute_statement(
        self, statement: str, query_name: str, print_statement=False
    ) -> None:
        self.layer_out: AWSRedshiftLayer
        if print_statement:
            logging.info((len(query_name) + 4) * "*")
            logging.info(f"* {query_name} *")
            logging.info((len(query_name) + 4) * "*")
            logging.info(statement)
            logging.info((len(query_name) + 4) * "*")
        self.layer_out.conn.execute(statement)

    def create_spectrum_table(self, step: ADFStep) -> None:
        self.layer_in: AWSBaseEMRLayer
        self.layer_out: AWSRedshiftLayer
        self.layer_out.validate_step(step)
        table_name = self.layer_out.get_table_name(step)
        self.execute_statement(
            "\n".join(
                [
                    "CREATE EXTERNAL SCHEMA IF NOT EXISTS SPECTRUM",
                    "FROM DATA CATALOG",
                    "DATABASE 'spectrumdb'",
                    f"IAM_ROLE '{self.layer_out.role_arn}'",
                    "CREATE EXTERNAL DATABASE IF NOT EXISTS;",
                ]
            ),
            "CREATE EXTERNAL SCHEMA",
        )
        self.execute_statement(
            f'DROP TABLE IF EXISTS spectrum."{table_name}";', "DROP EXTERNAL TABLE"
        )
        meta_dict = step.meta.get_column_dict()
        partition_cols = self.layer_in.get_step_partition_key(step)
        partition_string = ", ".join(
            f"{col} {self.redshift_type_map[meta_dict[col].cast] if (col in meta_dict) else 'VARCHAR(MAX)'}"
            for col in self.layer_in.get_step_partition_key(step)
        )
        step_format = self.layer_in.get_step_format(step)
        storage_format = "TEXTFILE" if step_format == "csv" else step_format.upper()
        self.execute_statement(
            "\n".join(
                [
                    f'CREATE EXTERNAL TABLE spectrum."{table_name}"(',
                    f"{ADFGlobalConfig.TIMESTAMP_COLUMN_NAME} VARCHAR(MAX),",
                    ",\n".join(
                        f"{col.name} {self.redshift_type_map[col.cast]}"
                        for col in step.meta.columns
                        if col not in partition_cols
                    ),
                    ")",
                    f"PARTITIONED BY ({partition_string})",
                    *(
                        ["ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"]
                        if step_format == "csv"
                        else []
                    ),
                    f"STORED AS {storage_format}",
                    f"LOCATION 's3://{self.layer_in.bucket}/{self.layer_in.get_step_prefix(step)}'",
                    *(
                        ["TABLE PROPERTIES ('skip.header.line.count'='1');"]
                        if step_format == "csv"
                        else [";"]
                    ),
                ]
            ),
            "CREATE EXTERNAL TABLE",
        )
        try:
            self.execute_statement(
                f'DROP TABLE IF EXISTS "{table_name}";', "DROP TABLE"
            )
        except ProgrammingError as e:
            if "is not a table" not in str(e):
                raise e
        self.execute_statement(f'DROP VIEW IF EXISTS "{table_name}";', "DROP VIEW")
        self.execute_statement(
            "\n".join(
                [
                    f'CREATE OR REPLACE VIEW "{table_name}"',
                    "AS SELECT *,",
                    f"NULL AS {ADFGlobalConfig.SQL_PK_COLUMN_NAME}",
                    f'FROM spectrum."{table_name}"',
                    "WITH NO SCHEMA BINDING;",
                ]
            ),
            "CREATE VIEW",
        )

    # TODO : key likely needs transforming for certain cases (whitespace, backslash, escape characters etc.)
    @staticmethod
    def key_to_value(val: str) -> str:
        return val

    def add_step_partitions(self, step: ADFStep):
        self.layer_in: AWSBaseEMRLayer
        self.layer_out: AWSRedshiftLayer
        table_name = self.layer_out.get_table_name(step)
        keys = s3_list_objects(
            self.layer_in.bucket, self.layer_in.get_step_prefix(step)
        )
        locations = list(
            set(
                os.path.dirname(key) + "/"
                for key in keys
                if key.endswith(f".{self.layer_in.get_step_format(step)}")
            )
        )
        statement_list = [f'ALTER TABLE spectrum."{table_name}" ADD IF NOT EXISTS']
        for location in locations:
            new_partition = {
                dir.split("=")[0]: self.key_to_value(dir.split("=")[1])
                for dir in location[len(self.layer_in.get_step_prefix(step)) :].split(
                    "/"
                )
                if "=" in dir
            }
            partition_string = ", ".join(
                f"{col}='{val}'" for col, val in new_partition.items()
            )
            statement_list.append(f"PARTITION({partition_string})")
            statement_list.append(f"LOCATION 's3://{self.layer_in.bucket}/{location}'")
        statement_list.append(";")
        statement = "\n".join(statement_list)
        self.execute_statement(statement, f"ADD PARTITIONS {locations}")

    def write_batch_data(
        self, ads: SparkDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.layer_in: AWSBaseEMRLayer
        self.layer_in.write_batch_data(ads, step, batch_id)
        self.add_step_partitions(step)

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SQLDataStructure:
        self.layer_out: AWSRedshiftLayer
        self.add_step_partitions(step)
        return self.layer_out.read_batch_data(step, batch_id)

    def read_batches_data(
        self, step: ADFStep, batch_ids: List[str]
    ) -> Optional[SQLDataStructure]:
        if not batch_ids:
            return None
        self.layer_out: AWSRedshiftLayer
        self.add_step_partitions(step)
        return self.layer_out.read_batches_data(step, batch_ids)

    def read_full_data(self, step: ADFStep) -> SQLDataStructure:
        self.layer_out: AWSRedshiftLayer
        self.add_step_partitions(step)
        return self.layer_out.read_full_data(step)

    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        self.create_spectrum_table(step_out)

    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        self.create_spectrum_table(step_in)

    def delete_step_write_out(self, step: ADFStep):
        self.layer_in.delete_step(step)

    def delete_step_read_in(self, step: ADFStep):
        self.layer_out.delete_step(step)
        for upstream_step in step.get_upstream_steps():
            self.layer_in.delete_step(upstream_step)

    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        self.layer_in.delete_batch(step, batch_id)

    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)
        for upstream_step in step.get_upstream_steps():
            self.layer_in.delete_batch(upstream_step, batch_id)


class EMRToAthenaTransition(ADLTransition):
    def default_to_write_out(self) -> bool:
        return True

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [AWSBaseEMRLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [AWSAthenaLayer]

    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        self.layer_in: AWSBaseEMRLayer
        self.layer_out: AWSAthenaLayer
        self.layer_out.create_external_table(
            step=step_out,
            bucket=self.layer_in.bucket,
            s3_prefix=self.layer_in.get_step_prefix(step_out),
            step_format=self.layer_in.get_step_format(step_out),
            partition_cols=self.layer_in.get_step_partition_key(step_out),
        )

    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        raise NotImplementedError(f"Setting up read in unsupported in {str(self)} !")

    def delete_step_write_out(self, step: ADFStep):
        self.layer_in.delete_step(step)

    def delete_step_read_in(self, step: ADFStep):
        raise NotImplementedError(f"Deleting step read in unsupported in {str(self)} !")

    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        self.layer_in.delete_batch(step, batch_id)

    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        raise NotImplementedError(
            f"Deleting batch read in unsupported in {str(self)} !"
        )

    def read_batch_data(self, step: ADFStep, batch_id: str) -> SparkDataStructure:
        self.layer_in: AWSBaseEMRLayer
        return self.layer_in.read_batch_data(step, batch_id)

    def read_full_data(self, step: ADFStep) -> SparkDataStructure:
        self.layer_in: AWSBaseEMRLayer
        return self.layer_in.read_full_data(step)

    def write_batch_data(
        self, ads: SparkDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.layer_in: AWSBaseEMRLayer
        self.layer_in.write_batch_data(ads, step, batch_id)
        # TODO : fix athena connectivity inside VPC
        # self.layer_out: AWSAthenaLayer
        # self.layer_out.repair_partitions(step)


class AthenaToAthenaTransition(ADLTransition):
    def validate(self) -> None:
        super().validate()
        self.layer_in: AWSAthenaLayer
        self.layer_out: AWSAthenaLayer
        if self.layer_in.db_name != self.layer_out.db_name:
            raise ValueError(f"Cannot create transition {self} across different DBs")

    def default_to_write_out(self) -> bool:
        return True

    @staticmethod
    def get_handled_layers_in() -> List[Type[AbstractDataLayer]]:
        return [AWSAthenaLayer]

    @staticmethod
    def get_handled_layers_out() -> List[Type[AbstractDataLayer]]:
        return [AWSAthenaLayer]

    def setup_write_out(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def setup_read_in(self, step_in: ADFStep, step_out: ADFStep) -> None:
        pass

    def delete_step_write_out(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_step_read_in(self, step: ADFStep):
        self.layer_out.delete_step(step)

    def delete_batch_write_out(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)

    def delete_batch_read_in(self, step: ADFStep, batch_id: str):
        self.layer_out.delete_batch(step, batch_id)

    def read_batch_data(self, step: ADFStep, batch_id: str) -> AbstractDataStructure:
        return self.layer_in.read_batch_data(step, batch_id)

    def read_full_data(self, step: ADFStep) -> AbstractDataStructure:
        return self.layer_in.read_full_data(step)

    def write_batch_data(
        self, ads: AbstractDataStructure, step: ADFStep, batch_id: str
    ) -> None:
        self.layer_out.write_batch_data(ads, step, batch_id)

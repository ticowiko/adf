import logging

from abc import ABC, abstractmethod

from typing import Dict, Optional, List

from sqlalchemy import Column, BigInteger
from sqlalchemy.schema import CreateColumn
from sqlalchemy.ext.compiler import compiles

from ADF.exceptions import AWSUnmanagedOperation
from ADF.components.data_structures import SQLDataStructure
from ADF.components.flow_config import ADFStep
from ADF.components.layers import PostgreSQLDataLayer
from ADF.utils import AWSRedshiftConfig, AWSRedshiftConnector
from ADF.config import ADFGlobalConfig


@compiles(CreateColumn)
def compile(element, compiler, **kwargs):
    column = element.element
    if "IDENTITY" not in column.info:
        return compiler.visit_create_column(element, **kwargs)
    return f"{column.name} {compiler.type_compiler.process(column.type)} IDENTITY {str(column.info['IDENTITY'])}"


class AWSRedshiftLayer(PostgreSQLDataLayer, ABC):
    def __init__(
        self,
        as_layer: str,
        table_prefix: str,
        endpoint: Optional[str] = None,
        port: Optional[int] = None,
        db_name: Optional[str] = None,
        user: Optional[str] = None,
        role_arn: Optional[str] = None,
    ):
        super().__init__(
            as_layer=as_layer,
            engine="redshift+psycopg2",
            host=endpoint,
            port=port,
            db=db_name,
            user=user,
            pw=AWSRedshiftConfig.get_master_password(),
            table_prefix=table_prefix,
        )
        self.role_arn = role_arn

    def get_step_table_creation_kwargs(self, step: ADFStep) -> Dict:
        return {
            "include_serial_pk": False,
            ADFGlobalConfig.SQL_PK_COLUMN_NAME: Column(
                BigInteger, primary_key=True, info={"IDENTITY": (0, 1)}
            ),
        }

    def attach_partition(self, ads: SQLDataStructure, step: ADFStep):
        logging.warning(
            f"WARNING : {self.__class__.__name__} does not support partitioning, ignoring for step {step}."
        )

    @staticmethod
    def add_partition_to_pk() -> bool:
        return False

    @abstractmethod
    def setup_layer(self) -> None:
        pass

    @abstractmethod
    def destroy(self) -> None:
        pass

    def list_tables(self) -> List[str]:
        return [
            result[0]
            for result in self.conn.execute(
                f"SELECT tablename FROM PG_TABLE_DEF WHERE tablename LIKE '{self.table_prefix}%%';"
            )
        ]


class ManagedAWSRedshiftLayer(AWSRedshiftLayer):
    def __init__(
        self,
        as_layer: str,
        table_prefix: str,
        identifer: str,
        role_arn: str,
        db_name: Optional[str] = None,
        number_of_nodes: Optional[int] = None,
        node_type: Optional[str] = None,
        sg_id: Optional[str] = None,
        cluster_subnet_group_name: Optional[str] = None,
    ):
        self.redshift_connector = AWSRedshiftConnector(
            identifier=identifer,
            role_arn=role_arn,
            db_name=db_name,
            number_of_nodes=number_of_nodes,
            node_type=node_type,
            sg_id=sg_id,
            cluster_subnet_group_name=cluster_subnet_group_name,
        )
        config = self.redshift_connector.fetch_config()
        if config is None:
            super().__init__(
                as_layer=as_layer,
                table_prefix=table_prefix,
            )
        else:
            super().__init__(
                as_layer=as_layer,
                endpoint=config.endpoint,
                port=config.port,
                db_name=config.db_name,
                user=config.master_user,
                table_prefix=table_prefix,
                role_arn=role_arn,
            )

    def setup_layer(self) -> None:
        config: AWSRedshiftConfig = self.redshift_connector.update_or_create()
        self.host = config.endpoint
        self.port = config.port
        self.db = config.db_name
        self.user = config.master_user

    def destroy(self) -> None:
        self.redshift_connector.destroy_if_exists()

    def output_prebuilt_config(self) -> Dict[str, str]:
        return {
            "table_prefix": self.table_prefix,
            "endpoint": self.host,
            "port": self.port,
            "db_name": self.db,
            "user": self.user,
            "role_arn": self.redshift_connector.role_arn,
        }


class PrebuiltAWSRedshiftLayer(AWSRedshiftLayer):
    def __init__(
        self,
        as_layer: str,
        table_prefix: str,
        endpoint: Optional[str] = None,
        port: Optional[int] = None,
        db_name: Optional[str] = None,
        user: Optional[str] = None,
        role_arn: Optional[str] = None,
    ):
        super().__init__(
            as_layer=as_layer,
            table_prefix=table_prefix,
            endpoint=endpoint,
            port=port,
            db_name=db_name,
            user=user,
            role_arn=role_arn,
        )

    def setup_layer(self) -> None:
        raise AWSUnmanagedOperation(self, "setup_layer")

    def destroy(self) -> None:
        raise AWSUnmanagedOperation(self, "destroy")

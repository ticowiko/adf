import os
import sys

import boto3

import ADF


def get_aws_region() -> str:
    try:
        region = boto3.session.Session().region_name or os.environ.get(
            "AWS_DEFAULT_REGION", "eu-west-1"
        )
    except:
        region = "eu-west-1"
    return region


class ADFGlobalConfig:
    # Root path
    ADF_WORKING_DIR = os.environ.get("ADF_WORKING_DIR", ".")

    # Columns
    BATCH_ID_COLUMN_NAME = "adf_batch_id"
    TIMESTAMP_COLUMN_NAME = "adf_timestamp"
    SQL_PK_COLUMN_NAME = "adf_sql_id"
    UUID_TAG_COLUMN_NAME = "adf_uuid_tag"

    # Statuses
    STATUS_SUBMITTED = "submitted"
    STATUS_RUNNING = "running"
    STATUS_FAILED = "failed"
    STATUS_SUCCESS = "success"
    STATUS_DELETING = "deleting"
    STATUSES = [
        STATUS_SUBMITTED,
        STATUS_RUNNING,
        STATUS_FAILED,
        STATUS_SUCCESS,
        STATUS_DELETING,
    ]

    # AWS settings
    AWS_REGION = get_aws_region()
    AWS_PACKAGE_BUCKET = "adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783"

    # Defaults
    BATCH_TIME_FORMAT_STRING = "%Y_%m_%d__%H_%M_%S"

    @classmethod
    def get_tech_cols(cls):
        return [
            cls.BATCH_ID_COLUMN_NAME,
            cls.TIMESTAMP_COLUMN_NAME,
            cls.SQL_PK_COLUMN_NAME,
        ]

    @classmethod
    def get_artifact_prefix(cls) -> str:
        return f"public/artifacts/v{ADF.__version__}/python{sys.version_info.major}.{sys.version_info.minor}/"

    @classmethod
    def get_adf_launcher_key(cls) -> str:
        return f"{cls.get_artifact_prefix()}adf-launcher.py"

    @classmethod
    def get_emr_package_zip_key(cls) -> str:
        return cls.get_artifact_prefix() + "zips/emr_package.zip"

    @classmethod
    def get_lambda_package_zip_key(cls) -> str:
        return cls.get_artifact_prefix() + "zips/lambda_package.zip"

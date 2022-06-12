import os


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
    AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "eu-west-3")
    AWS_PACKAGE_BUCKET = "adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783"
    AWS_EMR_PACKAGE_KEY = "public/zips/emr_package.zip"
    AWS_LAMBDA_PACKAGE_KEY = "public/zips/lambda_package.zip"

    # Defaults
    BATCH_TIME_FORMAT_STRING = "%Y_%m_%d__%H_%M_%S"

    @classmethod
    def get_tech_cols(cls):
        return [
            cls.BATCH_ID_COLUMN_NAME,
            cls.TIMESTAMP_COLUMN_NAME,
            cls.SQL_PK_COLUMN_NAME,
        ]

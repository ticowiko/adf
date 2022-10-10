import logging

from typing import List, Tuple

from ADF.utils.aws_utils.aws_clients import s3_resource, s3_client


def s3_delete_prefix(bucket: str, prefix: str, ignore_nosuchbucket: bool = False):
    try:
        logging.info(f"Deleting s3 objects '{bucket}/{prefix}*'...")
        s3_resource.Bucket(bucket).objects.filter(Prefix=prefix).delete()
    except s3_client.exceptions.NoSuchBucket as e:
        if ignore_nosuchbucket:
            logging.info(
                f"Skipping deletion of files in {bucket}/{prefix} as the bucket does not exist."
            )
        else:
            raise e


def s3_list_objects(bucket: str, prefix: str) -> List[str]:
    return [
        result["Key"]
        for result_page in s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix=prefix,
        )
        for result in result_page.get("Contents", [])
    ]


def s3_list_folders(bucket: str, prefix: str) -> List[str]:
    return [
        result["Prefix"]
        for result_page in s3_client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/",
        )
        for result in result_page.get("CommonPrefixes", [])
    ]


def s3_url_to_bucket_and_key(path: str) -> Tuple[str, str]:
    if not path.startswith("s3://"):
        raise ValueError(
            f"Cannot parse s3 path {path} as it does not start with 's3://'"
        )
    return path[5:].split("/")[0], path[5:][path[5:].find("/") + 1 :]

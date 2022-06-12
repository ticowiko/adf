import logging

from typing import List

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
        for result_page in s3_client.get_paginator("list_objects").paginate(
            Bucket=bucket,
            Prefix=prefix,
        )
        for result in result_page.get("Contents", [])
    ]

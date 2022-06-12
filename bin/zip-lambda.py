#!/usr/bin/env python3

import os
import sysconfig

from argparse import ArgumentParser
from zipfile import ZipFile, ZIP_DEFLATED

from ADF.utils import run_command, zip_files


if __name__ == "__main__":
    parser = ArgumentParser("Create zip for AWS EMR layers")
    parser.add_argument(
        "-r",
        "--root-path",
        metavar="ROOT-PATH",
        type=str,
        help="root path from which to zip",
        default=".",
    )
    parser.add_argument(
        "-o",
        "--output-path",
        metavar="OUTPUT-PATH",
        type=str,
        help="destination path for zip",
        default="lambda_package.zip",
    )
    args = parser.parse_args()

    print("Zipping code for lambda...", flush=True)
    run_command("find . -name '*.pyc' -delete")
    zip_handler = ZipFile(args.output_path, "w", ZIP_DEFLATED)
    zip_files(
        zip_hanlder=zip_handler,
        path=sysconfig.get_paths()["purelib"],
        exclude=lambda x: x.endswith(".jar")
        or x.startswith("pyarrow/")
        or x.startswith("pip/")
        or x.startswith("IPython/")
        or x.startswith("boto3/")
        or x.startswith("botocore/")
        or x.startswith("django/")
        or x.startswith("adf-")
        or x.startswith("adf_"),
    )
    zip_files(
        zip_hanlder=zip_handler,
        path=os.path.join(args.root_path, "src"),
        exclude=lambda x: not x.startswith("ADF/") or x.endswith(".zip"),
    )
    zip_files(
        zip_hanlder=zip_handler,
        path=os.path.join(args.root_path, "src", "ADF", "aws_lambda_handlers.py"),
    )
    zip_handler.close()

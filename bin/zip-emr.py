#!/usr/bin/env python3

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
        default="emr_package.zip",
    )
    args = parser.parse_args()

    print("Zipping code for emr...", flush=True)
    run_command("find . -name '*.pyc' -delete")
    zip_handler = ZipFile(args.output_path, "w", ZIP_DEFLATED)
    zip_files(
        zip_hanlder=zip_handler,
        path=args.root_path,
        exclude=lambda x: x.endswith(".zip")
        or x.endswith(".gz")
        or x.endswith("key-pair")
        or x.startswith("build/")
        or x.startswith("dist/")
        or x.startswith("local_implementers/")
        or x.startswith(".idea/")
        or x.startswith(".git/"),
    )
    zip_handler.close()

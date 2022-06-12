#!/usr/bin/env python3

import os
import stat
import glob
import pkgutil

from pathlib import Path
from argparse import ArgumentParser


if __name__ == "__main__":
    parser = ArgumentParser("Initialize ADF workspace")
    parser.add_argument(
        "path",
        metavar="PATH",
        type=str,
    )
    args = parser.parse_args()

    # Create root dir, fail if exists
    os.mkdir(args.path)

    # Create subdirectory structure
    Path(os.path.join(args.path, "local_implementers/logs/aws")).mkdir(
        parents=True, exist_ok=True
    )
    Path(os.path.join(args.path, "config/flows")).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(args.path, "config/implementers")).mkdir(
        parents=True, exist_ok=True
    )
    Path(os.path.join(args.path, "data_samples")).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(args.path, "scripts")).mkdir(parents=True, exist_ok=True)
    Path(os.path.join(args.path, "src/flow_operations")).mkdir(
        parents=True, exist_ok=True
    )
    open(os.path.join(args.path, "src/flow_operations/__init__.py"), "w").close()

    # Write files
    for source, target in [
        ("data/pyfiles/setup.py", "setup.py"),
        ("data/pyfiles/operations.py", "src/flow_operations/operations.py"),
        ("data/scripts/complete.sh", "scripts/complete.sh"),
        *[
            (f"data/scripts/{script}", f"scripts/{script}")
            for script in [
                "aws.sh",
                "complete.sh",
                "list.sh",
                "pandas.sh",
                "postgres.sh",
                "spark.sh",
                "sqlite.sh",
            ]
        ],
        *[
            (
                f"data/config/implementers/{implementer}",
                f"config/implementers/{implementer}",
            )
            for implementer in [
                "implementer.aws.yaml",
                "implementer.local-mono-layer.list.yaml",
                "implementer.local-mono-layer.pandas.yaml",
                "implementer.local-mono-layer.spark.yaml",
                "implementer.local-multi-layer.postgres.yaml",
                "implementer.local-multi-layer.sqlite.yaml",
            ]
        ],
        *[
            (f"data/config/flows/{flow}", f"config/flows/{flow}")
            for flow in [
                "flows.complete.yaml",
                "flows.local-mono-layer.yaml",
                "flows.local-multi-layer.yaml",
                "flows.simple.custom-flow-control.yaml",
                "flows.simple.default-flow-control.yaml",
                "flows.validation.errors.yaml",
            ]
        ],
        *[
            (f"data/data_samples/{sample}", f"data_samples/{sample}")
            for sample in ["test.csv"]
        ],
    ]:
        open(os.path.join(args.path, target), "w").write(
            pkgutil.get_data("ADF", source).decode("utf-8")
        )

    # Make scripts executable
    try:
        for path in glob.glob(f"{os.path.join(args.path, 'scripts')}/*.sh"):
            os.chmod(
                path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
            )
    except Exception as e:
        print(
            f"WARNING : Failed to make scripts executable, got {e.__class__.__name__} : {str(e)}"
        )

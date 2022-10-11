#!/usr/bin/env python3

import os
import re
import sys
import glob
from setuptools import setup, find_packages
from pathlib import Path

mo = re.search(
    r"^__version__ = ['\"]([^'\"]*)['\"]",
    open(os.path.join(os.path.dirname(__file__), "src/ADF/_version.py"), "r").read(),
    re.M,
)
if mo:
    version = mo.group(1)
    print(f"Parsed version : {version}")
else:
    raise RuntimeError("Failed to parse version")


requirements = [
    "numpy>=1.21.2",
    "pandas>=1.2.4",
    "pyarrow>=4.0.1",
    "pyspark>=3.1.1",
    "SQLAlchemy>=1.4.17",
    "sqlalchemy-redshift>=0.8.5",
    "pyathena>=2.14.0",
    "PyYAML>=6.0",
    "psycopg2-binary>=2.9.1",
    "boto3>=1.24.66",
    "croniter>=1.3.4",
    "venv-pack>=0.2.0",
]
if "--dev" in sys.argv:
    requirements += [
        "boto3-stubs>=1.24.66",
        "ipython>=7.27.0",
        "black>=22.10",
        "twine>=4.0",
    ]
    sys.argv.remove("--dev")
if "--emr" in sys.argv:
    print("USING EMR INSTALL PARAMETERS")
    requirements = [
        "py4j>=0.10.9.2",
        "numpy>=1.21.2",
        "pandas>=1.2.4",
        "SQLAlchemy>=1.4.17",
        "sqlalchemy-redshift>=0.8.5",
        "pyathena>=2.14.0",
        "PyYAML>=6.0",
        "psycopg2-binary>=2.9.1",
        "boto3>=1.18.31",
        "croniter>=1.3.4",
    ]
    sys.argv.remove("--emr")


setup(
    name="adf",
    version=version,
    description="Create infrastructure agnostic data processing pipelines",
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type='text/markdown',
    author="Patrick El Hage",
    author_email="patrickelhageuniv@gmail.com",
    packages=find_packages(where="src/"),
    package_dir={"": "src/"},
    install_requires=requirements,
    scripts=glob.glob("bin/*.py"),
    package_data={
        "ADF": [
            "data/config/flows/*.yaml",
            "data/config/implementers/*.yaml",
            "data/data_samples/*.csv",
            "data/pyfiles/*.py",
            "data/scripts/*.sh",
        ]
    },
)

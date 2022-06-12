#!/usr/bin/env python3

import sys
import glob
from setuptools import setup, find_packages


requirements = [
    "numpy==1.21.2",
    "pandas==1.2.4",
    "pyarrow==4.0.1",
    "pyspark==3.1.1",
    "SQLAlchemy==1.4.17",
    "sqlalchemy-redshift==0.8.5",
    "PyYAML>=6.0",
    "psycopg2-binary==2.9.1",
    "boto3==1.18.31",
    "croniter==1.3.4",
]
if "--dev" in sys.argv:
    requirements += [
        "boto3-stubs==1.18.36",
        "ipython==7.27.0",
    ]
    sys.argv.remove("--dev")
if "--emr" in sys.argv:
    print("USING EMR INSTALL PARAMETERS")
    requirements = [
        "py4j==0.10.9.2",
        "numpy==1.21.2",
        "pandas==1.2.4",
        "SQLAlchemy==1.4.17",
        "sqlalchemy-redshift==0.8.5",
        "PyYAML>=6.0",
        "psycopg2-binary==2.9.1",
        "boto3==1.18.31",
        "croniter==1.3.4",
    ]
    sys.argv.remove("--emr")


setup(
    name="adf",
    version="0.0.6",
    description="Create infrastructure agnostic data processing pipelines",
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

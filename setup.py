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


requirements_file = "requirements.txt"
if "--dev" in sys.argv:
    requirements_file = "requirements_dev.txt"
    sys.argv.remove("--dev")
if "--emr" in sys.argv:
    requirements_file = "requirements_emr.txt"
    sys.argv.remove("--emr")
requirements = list(filter(bool, open("requirements_dev.txt", "r").read().splitlines()))


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

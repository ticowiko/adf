#!/usr/bin/env python3

import sys
from setuptools import setup, find_packages


requirements = []
if "--emr" in sys.argv:
    print("USING EMR INSTALL PARAMETERS")
    requirements = []
    sys.argv.remove("--emr")


setup(
    name="adf_wksp",
    version="0.0.1",
    description="ADF workspace",
    author="Some Name",
    author_email="somename@service.domain",
    packages=find_packages(where="src/"),
    package_dir={"": "src/"},
    install_requires=requirements,
)

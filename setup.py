#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-fusebill",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Dante Perez",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_fusebill"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-fusebill=tap_fusebill:main
    """,
    packages=["tap_fusebill"],
    package_data = {
        "schemas": ["tap_fusebill/schemas/*.json"]
    },
    include_package_data=True,
)

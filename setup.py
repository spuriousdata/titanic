#!/usr/bin/env python3

import os
from setuptools import setup

setup(
    name="titanic",
    version='0.0.2',
    author="Mike O'Malley",
    author_email="spuriousdata@gmail.com",
    license="MIT",
    packages=['titanic'],
    install_requires=['PyYAML', 'boto3', 'tqdm'],
    entry_points={
        'console_scripts': ['titanic=titanic.__main__:main'],
    },
)

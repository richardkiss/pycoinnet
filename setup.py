#!/usr/bin/env python

from setuptools import setup

version = "0.19"

REQUIREMENTS = [i.strip() for i in open("requirements.txt").readlines()]

setup(
    name="pycoinnet",
    version=version,
    packages=[
        "pycoinnet",
        "pycoinnet.scripts",
    ],
    entry_points={'console_scripts': [
        'blockwatcher = pycoinnet.examples.blockwatcher:main',
    ]},
    install_requires=REQUIREMENTS,
    author="Richard Kiss",
    author_email="him@richardkiss.com",
    url="https://github.com/richardkiss/pycoinnet",
    license="http://opensource.org/licenses/MIT",
    description="Network utilities for communicating on the bitcoin network."
)

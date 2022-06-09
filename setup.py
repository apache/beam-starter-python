# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from distutils.core import setup
from setuptools import find_packages

with open("requirements.txt") as f:
    requirements = f.readlines()

setup(
    # package_dir={"": "src"},
    # name="Distutils",
    # version="1.0",
    # description="Python Distribution Utilities",
    # author="Greg Ward",
    # author_email="gward@python.net",
    # url="https://www.python.org/sigs/distutils-sig/",
    # packages=["distutils", "distutils.command"],
    install_requires=requirements,
    package_dir={"": "src"},
)

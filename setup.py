# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = [line.strip() for line in f.readlines() if line.strip() and not line.startswith('#')]

setup(
    name="my-app",
    version="1.0.0",
    description="Python Apache Beam pipeline.",
    author="My name",
    author_email="my@email.com",
    packages=find_packages(),
    install_requires=requirements + ["setuptools>=80.9.0"],
    python_requires=">=3.8",
)

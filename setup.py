# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from setuptools import setup, find_packages

requirements = [
    "apache-beam[gcp]==2.42.0", "transformers==4.21.0", "torch==1.13.0", "torchvision==0.14.0"
]

setup(
    name="My app",
    version="1.0",
    description="Python Apache Beam pipeline.",
    author="Danny McCormick",
    author_email="my@email.com",
    packages=find_packages(),
    install_requires=requirements,
)

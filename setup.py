#!/usr/bin/env python3

from setuptools import setup

__version = "2.0.2"

setup(name="oc-checksums-worker",
        version=__version,
        description="Checksums worker for AMQP queue",
        long_description="Checksums Django-based worker for AMQP queue",
        long_description_content_type="text/plain",
        license="Apache2.0",
        install_requires=[
            "oc-checksumsq",
            "oc-delivery-apps >= 11.2.9",
            "oc-orm-initializator",
            "oc-cdtapi"],
      packages={"oc_checksums_worker"},
      python_requires=">=3.6")

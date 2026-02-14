#!/usr/bin/env python
"""
Setup script for Apex Risk Platform library.

This library is packaged and uploaded to S3 as a zip file
for use by AWS Glue jobs via --extra-py-files.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="apex_risk_platform",
    version="3.2.1",
    author="Apex Financial Data Engineering Team",
    author_email="data-engineering@apexfinancial.com",
    description="Risk Analytics Platform for AWS Glue ETL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apex-financial/risk-platform",
    packages=find_packages(exclude=["tests", "tests.*", "scripts"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "boto3>=1.26.0",
        "pyyaml>=6.0",
        "requests>=2.28.0",
        "python-dateutil>=2.8.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.10.0",
            "moto>=4.0.0",
            "flake8>=5.0.0",
            "black>=22.0.0",
            "mypy>=0.990",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
    },
    package_data={
        "apex_risk_platform": [
            "config/*.yaml",
            "workflows/*.json",
        ],
    },
    entry_points={
        "console_scripts": [
            "apex-risk=apex_risk_platform.cli:main",
        ],
    },
)

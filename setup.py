from setuptools import setup, find_packages
import os

# Read version from __init__.py
def get_version():
    init_file = os.path.join('glue2lakehouse', '__init__.py')
    with open(init_file, 'r') as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"').strip("'")
    return "1.0.0"

# Read README
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith('#')]

# Development dependencies
dev_requirements = [
    "pytest>=7.0.0",
    "pytest-cov>=3.0.0",
    "black>=22.0.0",
    "flake8>=4.0.0",
    "mypy>=0.950",
    "isort>=5.10.0",
    "sphinx>=4.5.0",
    "sphinx-rtd-theme>=1.0.0",
]

setup(
    name="glue2lakehouse",
    version=get_version(),
    author="Analytics360",
    author_email="contact@analytics360.com",
    description="AI-powered AWS Glue to Databricks Lakehouse migration framework with dual-mode (rule-based + LLM) intelligence",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/krishkilaru-arch/glue2lakehouse",
    project_urls={
        "Bug Tracker": "https://github.com/krishkilaru-arch/glue2lakehouse/issues",
        "Documentation": "https://glue2lakehouse.readthedocs.io",
        "Source Code": "https://github.com/krishkilaru-arch/glue2lakehouse",
    },
    packages=find_packages(exclude=[
        "tests", "tests.*",
        "examples", "examples.*",
        "docs", "docs.*",
        "recycle", "recycle.*",
        "databricks_app", "databricks_app.*"
    ]),
    include_package_data=True,
    package_data={
        "glue2lakehouse": [
            "config.yaml",
            "mappings/*.yaml",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: System :: Archiving :: Mirroring",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
        "Typing :: Typed",
    ],
    keywords="aws glue databricks migration etl spark pyspark code-transformation",
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=3.0.0",
        ],
        "docs": [
            "sphinx>=4.5.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "glue2lakehouse=glue2lakehouse.cli:cli",
            "g2lh=glue2lakehouse.cli:cli",  # Short alias
        ],
    },
    zip_safe=False,
)

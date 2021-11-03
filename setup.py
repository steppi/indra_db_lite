from os import path
from setuptools import setup, find_packages


here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    name="indra_db_lite",
    version="0.0.0",
    description="Work with content from indra_db in a local sqlite database.",
    author="indra_db_lite developers, Harvard Medical School",
    author_email="albert_steppi@hms.harvard.edu",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    packages=find_packages(),
    install_requires=["boto3"],
    extras_require={
        "assemble": ["indra_db", "lxml", "requests", "sqlalchemy"]
    },
    include_package_data=True,
)

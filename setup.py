import os

from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="sql-scheduler",
    author="Henry Jones",
    author_email="henryivesjones@gmail.com",
    url="https://github.com/henryivesjones/sql-scheduler",
    description="sql-scheduler allows you to easily run a suite of SQL scripts against a Postgres/Redshift database.",
    packages=["sql_scheduler"],
    package_dir={"sql_scheduler": "sql_scheduler"},
    package_data={"sql_scheduler": ["py.typed"]},
    include_package_data=True,
    long_description=read("README.md"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
    ],
)

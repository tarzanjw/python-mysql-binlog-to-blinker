#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

# requirements
install_requires = [
    "mysql-replication>=0.7",
    "blinker>=1.4",
    'six',
]

dev_requires = ["pytest",
                "tox",
                ] + install_requires

setup(name="pymysqlblinker",
      version=__import__("pymysqlblinker").__version__,
      description="mysql binlog to blinker signal",
      keywords="event replication mysql signal",
      author="Hoc .T Do",
      author_email="hoc3010@gmail.com",
      packages=['pymysqlblinker', ],
      url="https://github.com/tarzanjw/python-mysql-replication-blinker",
      license="MIT",
      zip_safe=False,
      long_description=open("README.rst").read(),
      install_requires=install_requires,
      extras_require={
          "dev": dev_requires,
      },
      classifiers=[
          "Topic :: Software Development",
          "Development Status :: 3 - Alpha",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: MIT License",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3.4",
      ]
      )

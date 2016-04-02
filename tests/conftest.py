# -*- coding: utf-8 -*-

from __future__ import absolute_import
import logging
logging.basicConfig(level=logging.DEBUG)

import json
import os

import pymysql
import pytest


@pytest.fixture(scope="session")
def conf():
    """Try load local conf.json
    """
    fname = os.path.join(os.path.dirname(__file__), "conf.json")
    if os.path.exists(fname):
        with open(fname) as f:
            return json.load(f)


@pytest.fixture(scope="module")
def mysql_settings(conf):
    """MySQL server dsn

    This fixture will init a clean tbl0 database with a 'test' table
    """
    logger = logging.getLogger("fixture_mysql_dsn")

    # init database
    db_settings = conf['mysql_settings']
    conn = pymysql.connect(**db_settings)
    cursor = conn.cursor()

    conn.begin()
    cursor.execute("DROP DATABASE IF EXISTS testdb")
    cursor.execute("CREATE DATABASE testdb")
    cursor.execute("DROP TABLE IF EXISTS testdb.tbl0")
    cursor.execute('''CREATE TABLE testdb.tbl0 (
                        id INT NOT NULL AUTO_INCREMENT,
                        data VARCHAR (256) NOT NULL,
                        PRIMARY KEY (id)
                   )''')
    cursor.execute("RESET MASTER")
    conn.commit()

    logger.debug("executed")

    # release conn
    cursor.close()
    conn.close()

    return db_settings


@pytest.fixture(scope="class")
def class_mysql_settings(request, mysql_settings):
    # set a class attribute on the invoking test context
    request.cls.mysql_settings = mysql_settings
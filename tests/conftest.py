# -*- coding: utf-8 -*-

from __future__ import absolute_import
from six.moves.urllib.parse import urlparse
# from urlparse import urlparse
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
def mysql_dsn(conf):
    """MySQL server dsn

    This fixture will init a clean meepo_test database with a 'test' table
    """
    logger = logging.getLogger("fixture_mysql_dsn")

    dsn = conf["mysql_dsn"] if conf else \
        "mysql+pymysql://root@localhost/"

    # init database
    parsed = urlparse(dsn)
    db_settings = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "passwd": parsed.password
    }
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

    return dsn

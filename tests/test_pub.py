# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
from six.moves.urllib.parse import urlparse
from pymysqlreplication.row_event import \
    WriteRowsEvent, \
    UpdateRowsEvent, \
    DeleteRowsEvent
from pymysqlblinker import signals, pub

logging.basicConfig(level=logging.DEBUG)

import pymysql
import pytest

t_binlogs = []
t_writes, t_updates, t_deletes = [], [], []
t_rows_writes, t_rows_updates, t_rows_deletes = [], [], []
t_row_writes, t_row_updates, t_row_deletes = [], [], []


def setup_module(module):
    def test_sg(sg_list):
        return lambda pk: sg_list.append(pk)

    # connect binlog event signals
    signals.binlog_write('testdb', 'tbl0').connect(test_sg(t_writes), weak=False)
    signals.binlog_update('testdb', 'tbl0').connect(test_sg(t_updates), weak=False)
    signals.binlog_delete('testdb', 'tbl0').connect(test_sg(t_deletes), weak=False)

    # connect rows signal
    signals.rows_write('testdb', 'tbl0').connect(test_sg(t_rows_writes), weak=False)
    signals.rows_update('testdb', 'tbl0').connect(test_sg(t_rows_updates), weak=False)
    signals.rows_delete('testdb', 'tbl0').connect(test_sg(t_rows_deletes), weak=False)

    # connect row signal
    signals.row_write('testdb', 'tbl0').connect(test_sg(t_row_writes), weak=False)
    signals.row_update('testdb', 'tbl0').connect(test_sg(t_row_updates), weak=False)
    signals.row_delete('testdb', 'tbl0').connect(test_sg(t_row_deletes), weak=False)

    # connect mysql binlog pos signal
    signals.binlog_pos_signal.connect(test_sg(t_binlogs),  weak=False)


@pytest.fixture(scope="module")
def binlog(mysql_dsn):
    # init mysql connection
    parsed = urlparse(mysql_dsn)
    db_settings = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "passwd": parsed.password,
        "database": "testdb"
    }
    conn = pymysql.connect(**db_settings)
    cursor = conn.cursor()

    # test sqls
    sql = """
    USE testdb;
    INSERT INTO tbl0 (data) VALUES ('a');
    INSERT INTO tbl0 (data) VALUES ('b'), ('c'), ('d');
    UPDATE tbl0 SET data = 'aa' WHERE id = 1;
    UPDATE tbl0 SET data = 'bb' WHERE id = 2;
    UPDATE tbl0 SET data = 'cc' WHERE id != 1;
    DELETE FROM tbl0 WHERE id != 1;
    DELETE FROM tbl0 WHERE id = 1;
    """
    cursor.execute(sql)
    cursor.close()
    conn.commit()
    conn.close()

    # generates signals
    pub.start_publishing(mysql_dsn)


def test_mysql_binlog_pos_event(binlog):
    assert all(pos[0] == "mysql-bin.000001" for pos in t_binlogs)


def test_mysql_table_event(binlog):
    assert len(t_writes) == 2
    assert all(isinstance(e, WriteRowsEvent) for e in t_writes)

    assert len(t_updates) == 3
    assert all(isinstance(e, UpdateRowsEvent) for e in t_updates)

    assert len(t_deletes) == 2
    assert all(isinstance(e, DeleteRowsEvent) for e in t_deletes)


def test_mysql_rows_event(binlog):
    assert t_rows_writes == [
        [
            {'values': {'data': 'a', 'id': 1}},
            ],
        [
            {'values': {'data': 'b', 'id': 2}},
            {'values': {'data': 'c', 'id': 3}},
            {'values': {'data': 'd', 'id': 4}},
            ]
    ]
    assert t_rows_updates == [
        [
            {'before_values': {'data': 'a', 'id': 1},
             'after_values': {'data': 'aa', 'id': 1}},
            ],
        [
            {'before_values': {'data': 'b', 'id': 2},
            'after_values': {'data': 'bb', 'id': 2}},
            ],
        [
            {'before_values': {'data': 'bb', 'id': 2},
             'after_values': {'data': 'cc', 'id': 2}},
            {'before_values': {'data': 'c', 'id': 3},
             'after_values': {'data': 'cc', 'id': 3}},
            {'before_values': {'data': 'd', 'id': 4},
             'after_values': {'data': 'cc', 'id': 4}},
            ],
    ]
    assert t_rows_deletes == [
        [
            {'values': {'data': 'cc', 'id': 2}},
            {'values': {'data': 'cc', 'id': 3}},
            {'values': {'data': 'cc', 'id': 4}},
            ],
        [
            {'values': {'data': 'aa', 'id': 1}},
            ],
    ]

def test_mysql_row_event(binlog):
    assert t_row_writes == [
        {'values': {'data': 'a', 'id': 1}},
        {'values': {'data': 'b', 'id': 2}},
        {'values': {'data': 'c', 'id': 3}},
        {'values': {'data': 'd', 'id': 4}},
    ]
    assert t_row_updates == [
        {'before_values': {'data': 'a', 'id': 1},
         'after_values': {'data': 'aa', 'id': 1}},
        {'before_values': {'data': 'b', 'id': 2},
         'after_values': {'data': 'bb', 'id': 2}},
        {'before_values': {'data': 'bb', 'id': 2},
         'after_values': {'data': 'cc', 'id': 2}},
        {'before_values': {'data': 'c', 'id': 3},
         'after_values': {'data': 'cc', 'id': 3}},
        {'before_values': {'data': 'd', 'id': 4},
         'after_values': {'data': 'cc', 'id': 4}},
    ]
    assert t_row_deletes == [
        {'values': {'data': 'cc', 'id': 2}},
        {'values': {'data': 'cc', 'id': 3}},
        {'values': {'data': 'cc', 'id': 4}},
        {'values': {'data': 'aa', 'id': 1}}
    ]

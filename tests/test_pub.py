# -*- coding: utf-8 -*-

from __future__ import absolute_import

try:
    # Python 3
    from urllib.parse import urlparse, parse_qsl
except ImportError:
    # Python 2
    from urlparse import urlparse, parse_qsl
from pymysqlreplication.row_event import \
    WriteRowsEvent, \
    UpdateRowsEvent, \
    DeleteRowsEvent
from pymysqlblinker import signals, pub

import pymysql
import pytest


t_binlogs = dict(zip(['write', 'update', 'delete'], [[], [], []]))
t_schemas = dict(zip(['write', 'update', 'delete'], [[], [], []]))
t_tables = dict(zip(['write', 'update', 'delete'], [[], [], []]))
t_rows = dict(zip(['write', 'update', 'delete'], [[], [], []]))
t_poses = []


def setup_module(module):
    def test_sg(sg_list):
        return lambda e, schema, table: sg_list.append((e, schema, table))

    # connect signals
    for act in ['write', 'update', 'delete']:
        # binlog signal
        getattr(signals, 'binlog_%s' % act).\
            connect(test_sg(t_binlogs[act]), weak=False)
        # schema signal
        getattr(signals, 'schema_%s' % act)('testdb').\
            connect(test_sg(t_schemas[act]), weak=False)
        # table signal
        getattr(signals, 'table_%s' % act)('testdb', 'tbl0').\
            connect(test_sg(t_tables[act]), weak=False)
        # row signal
        getattr(signals, 'row_%s' % act)('testdb', 'tbl0').\
            connect(test_sg(t_rows[act]), weak=False)

    # connect mysql binlog pos signal
    signals.binlog_pos_signal.connect(lambda e: t_poses.append(e),
                                      weak=False)


@pytest.fixture(scope="module")
def binlog(mysql_dsn):
    # init mysql connection
    parsed = urlparse(mysql_dsn)
    db_settings = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "passwd": parsed.password,
        "database": parsed.path.strip('/'),
    }
    db_settings.update(parse_qsl(parsed.query))
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


# def test_connection_timeout():
#     mysql_dsn = 'mysql+pymysql://root@10.1.2.12/nodb'
#     pub.start_publishing(mysql_dsn, connect_timeout=2)


def test_schema_table_name(binlog):
    for t_logs in [t_binlogs, t_schemas, t_tables, t_rows]:
        for logs in t_logs.values():
            assert all(e[1:] == ('testdb', 'tbl0') for e in logs)


def test_mysql_binlog_pos_signal(binlog):
    assert all(pos[0] == "mysql-bin.000001" for pos in t_poses)


def test_binlog_signal(binlog):
    assert all(isinstance(l[0], WriteRowsEvent) for l in t_binlogs['write'])
    assert all(isinstance(l[0], UpdateRowsEvent) for l in t_binlogs['update'])
    assert all(isinstance(l[0], DeleteRowsEvent) for l in t_binlogs['delete'])
    assert len(t_binlogs['write']) == 2
    assert len(t_binlogs['update']) == 3
    assert len(t_binlogs['delete']) == 2


def test_schema_signal(binlog):
    assert all(isinstance(l[0], WriteRowsEvent) for l in t_schemas['write'])
    assert all(isinstance(l[0], UpdateRowsEvent) for l in t_schemas['update'])
    assert all(isinstance(l[0], DeleteRowsEvent) for l in t_schemas['delete'])
    assert len(t_schemas['write']) == 2
    assert len(t_schemas['update']) == 3
    assert len(t_schemas['delete']) == 2


def test_table_signal(binlog):
    writes = [l[0] for l in t_tables['write']]
    updates = [l[0] for l in t_tables['update']]
    deletes = [l[0] for l in t_tables['delete']]
    assert writes == [
        [
            {'values': {'data': 'a', 'id': 1}},
            ],
        [
            {'values': {'data': 'b', 'id': 2}},
            {'values': {'data': 'c', 'id': 3}},
            {'values': {'data': 'd', 'id': 4}},
            ]
    ]
    assert updates == [
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
    assert deletes == [
        [
            {'values': {'data': 'cc', 'id': 2}},
            {'values': {'data': 'cc', 'id': 3}},
            {'values': {'data': 'cc', 'id': 4}},
            ],
        [
            {'values': {'data': 'aa', 'id': 1}},
            ],
    ]


def test_row_signal(binlog):
    writes = [l[0] for l in t_rows['write']]
    updates = [l[0] for l in t_rows['update']]
    deletes = [l[0] for l in t_rows['delete']]
    assert writes == [
        {'values': {'data': 'a', 'id': 1}},
        {'values': {'data': 'b', 'id': 2}},
        {'values': {'data': 'c', 'id': 3}},
        {'values': {'data': 'd', 'id': 4}},
    ]
    assert updates == [
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
    assert deletes == [
        {'values': {'data': 'cc', 'id': 2}},
        {'values': {'data': 'cc', 'id': 3}},
        {'values': {'data': 'cc', 'id': 4}},
        {'values': {'data': 'aa', 'id': 1}}
    ]

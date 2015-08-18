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
from pymysqlblinker import signals, start_publishing, start_replication
import unittest
import pymysql
import pytest
import tempfile


@pytest.mark.usefixtures("class_mysql_dsn")
class BaseTestCase(unittest.TestCase):
    def on_binlog_write(self, e, schema, table):
        self._sigs_binlogs['write'].append((e, schema, table))
    
    def on_binlog_update(self, e, schema, table):
        self._sigs_binlogs['update'].append((e, schema, table))
    
    def on_binlog_delete(self, e, schema, table):
        self._sigs_binlogs['delete'].append((e, schema, table))
    
    def on_schema_write(self, e, schema, table):
        self._sigs_schemas['write'].append((e, schema, table))
    
    def on_schema_update(self, e, schema, table):
        self._sigs_schemas['update'].append((e, schema, table))
    
    def on_schema_delete(self, e, schema, table):
        self._sigs_schemas['delete'].append((e, schema, table))
    
    def on_table_write(self, e, schema, table):
        self._sigs_tables['write'].append((e, schema, table))
    
    def on_table_update(self, e, schema, table):
        self._sigs_tables['update'].append((e, schema, table))
    
    def on_table_delete(self, e, schema, table):
        self._sigs_tables['delete'].append((e, schema, table))
    
    def on_row_write(self, e, schema, table):
        self._sigs_rows['write'].append((e, schema, table))
    
    def on_row_update(self, e, schema, table):
        self._sigs_rows['update'].append((e, schema, table))
    
    def on_row_delete(self, e, schema, table):
        self._sigs_rows['delete'].append((e, schema, table))

    def on_binlog_pos(self, e):
        self._sigs_poses.append(e)

    def _setupSignalSubscribers(self):
        self._sigs_binlogs = dict(zip(['write', 'update', 'delete'], [[], [], []]))
        self._sigs_schemas = dict(zip(['write', 'update', 'delete'], [[], [], []]))
        self._sigs_tables = dict(zip(['write', 'update', 'delete'], [[], [], []]))
        self._sigs_rows = dict(zip(['write', 'update', 'delete'], [[], [], []]))
        self._sigs_poses = []
        for stype in ['binlog', 'schema', 'table', 'row']:
            for saction in ['write', 'update', 'delete']:
                sig = getattr(signals, '%s_%s' % (stype, saction))
                if stype in ['table', 'row']:
                    sig = sig('testdb', 'tbl0')
                elif stype == 'schema':
                    sig = sig('testdb')
                sub = self.__getattribute__('on_%s_%s' % (stype, saction))
                sig.connect(sub)
        signals.binlog_pos_signal.connect(self.on_binlog_pos)

    def _teardownSignalSubscribers(self):
        for stype in ['binlog', 'schema', 'table', 'row']:
            for saction in ['write', 'update', 'delete']:
                sig = getattr(signals, '%s_%s' % (stype, saction))
                if stype in ['table', 'row']:
                    sig = sig('testdb', 'tbl0')
                elif stype == 'schema':
                    sig = sig('testdb')
                sub = self.__getattribute__('on_%s_%s' % (stype, saction))
                sig.disconnect(sub)
        signals.binlog_pos_signal.disconnect(self.on_binlog_pos)

    def _make_binlog(self):
        # init mysql connection
        parsed = urlparse(self.mysql_dsn)
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
        TRUNCATE TABLE tbl0;
        RESET MASTER;
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

    def setUp(self):
        self._setupSignalSubscribers()
        self._make_binlog()

    def tearDown(self):
        self._teardownSignalSubscribers()


class TestPublishing(BaseTestCase):
    def setUp(self):
        super(TestPublishing, self).setUp()
        start_publishing(self.mysql_dsn)

    def test_schema_table_name(self):
        for t_logs in [self._sigs_binlogs, self._sigs_schemas,
                       self._sigs_tables, self._sigs_rows]:
            for logs in t_logs.values():
                assert all(e[1:] == ('testdb', 'tbl0') for e in logs)

    def test_mysql_binlog_pos_signal(self):
        assert all(pos[0] == "mysql-bin.000001" for pos in self._sigs_poses)

    def test_binlog_signal(self):
        assert all(isinstance(l[0], WriteRowsEvent) for l in self._sigs_binlogs['write'])
        assert all(isinstance(l[0], UpdateRowsEvent) for l in self._sigs_binlogs['update'])
        assert all(isinstance(l[0], DeleteRowsEvent) for l in self._sigs_binlogs['delete'])
        assert len(self._sigs_binlogs['write']) == 2
        assert len(self._sigs_binlogs['update']) == 3
        assert len(self._sigs_binlogs['delete']) == 2

    def test_schema_signal(self):
        assert all(isinstance(l[0], WriteRowsEvent) for l in self._sigs_schemas['write'])
        assert all(isinstance(l[0], UpdateRowsEvent) for l in self._sigs_schemas['update'])
        assert all(isinstance(l[0], DeleteRowsEvent) for l in self._sigs_schemas['delete'])
        assert len(self._sigs_schemas['write']) == 2
        assert len(self._sigs_schemas['update']) == 3
        assert len(self._sigs_schemas['delete']) == 2

    def test_table_signal(self):
        writes = [l[0] for l in self._sigs_tables['write']]
        updates = [l[0] for l in self._sigs_tables['update']]
        deletes = [l[0] for l in self._sigs_tables['delete']]
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

    def test_row_signal(self):
        writes = [l[0] for l in self._sigs_rows['write']]
        updates = [l[0] for l in self._sigs_rows['update']]
        deletes = [l[0] for l in self._sigs_rows['delete']]
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


@pytest.fixture(scope='class')
def binlog_pos_storage_filename(request):
    # set a class attribute on the invoking test context
    import tempfile
    request.cls.binlog_pos_storage_filename = tempfile.mktemp(suffix='.binlog.pos')


@pytest.mark.usefixtures("binlog_pos_storage_filename")
class TestReplication(BaseTestCase):
    def _start_replication(self, resume_stream=False):
        start_replication(
            self.mysql_dsn,
            binlog_pos_storage_filename=self.binlog_pos_storage_filename,
            blocking=False,
            resume_stream=resume_stream,
        )

    def _make_more_binlog(self):
        # init mysql connection
        parsed = urlparse(self.mysql_dsn)
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

    def setUp(self):
        super(TestReplication, self).setUp()
        self._start_replication()

    def test_replication(self):
        import os

        # file storage should be exist
        assert os.path.isfile(self.binlog_pos_storage_filename)

        # get last position
        with open(self.binlog_pos_storage_filename, 'r') as f:
            pos = f.read()
            log_file, log_pos = pos.split(':')
            log_pos = int(log_pos)
        # clear current pos signals
        while self._sigs_poses:
            self._sigs_poses.pop()

        self._make_more_binlog()

        # continue replicating
        self._start_replication(resume_stream=True)

        # the just received pos signals must be greater than last saved pos
        for lf, lp in self._sigs_poses:
            assert lp > log_pos
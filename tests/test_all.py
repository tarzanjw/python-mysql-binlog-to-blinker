# -*- coding: utf-8 -*-

from __future__ import absolute_import

try:
    # Python 3
    from urllib.parse import urlparse, parse_qsl
except ImportError:
    # Python 2
    from urlparse import urlparse, parse_qsl
from pymysqlreplication import row_event
import pymysqlreplication
from mysqlbinlog2blinker import (
    signals,
    start_publishing,
    start_replication
)
import unittest
import pymysql
import pytest
import tempfile


@pytest.mark.usefixtures("class_mysql_settings")
class BaseTestCase(unittest.TestCase):
    def on_binlog_event(self, e, stream):
        self.binlog_events.append((e, stream))

    def on_rows_updated(self, table, rows, meta):
        assert table == 'testdb.tbl0'
        self.rows_updated_events.append((rows, meta))

    def on_rows_inserted(self, table, rows, meta):
        assert table == 'testdb.tbl0'
        self.rows_inserted_events.append((rows, meta))

    def on_rows_deleted(self, table, rows, meta):
        assert table == 'testdb.tbl0'
        self.rows_deleted_events.append((rows, meta))

    def on_binlog_position(self, e):
        self.position_events.append(e)

    def _setupSignalSubscribers(self):
        self.binlog_events = []
        self.rows_inserted_events = []
        self.rows_updated_events = []
        self.rows_deleted_events = []
        self.position_events = []

        signals.binlog_position_signal.connect(self.on_binlog_position)
        signals.binlog_signal.connect(self.on_binlog_event)
        signals.rows_inserted.connect(self.on_rows_inserted)
        signals.rows_updated.connect(self.on_rows_updated)
        signals.rows_deleted.connect(self.on_rows_deleted)

    def _teardownSignalSubscribers(self):
        signals.binlog_position_signal.disconnect(self.on_binlog_position)
        signals.binlog_signal.disconnect(self.on_binlog_event)
        signals.rows_inserted.disconnect(self.on_rows_inserted)
        signals.rows_updated.disconnect(self.on_rows_updated)
        signals.rows_deleted.disconnect(self.on_rows_deleted)

    def _make_binlog(self):
        # init mysql connection
        conn = pymysql.connect(**self.mysql_settings)
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
        start_publishing(self.mysql_settings)


    def _assert_meta(self, meta, action):
        # just ignore log_post, time in meta
        del meta['log_pos']
        del meta['time']
        self.assertDictEqual(meta, {
            'action': action,
            'log_file': u'mysql-bin.000001',
            'schema': u'testdb',
            'table': u'tbl0',
        })

    def test_mysql_binlog_pos_signal(self):
        assert all(pos[0] == "mysql-bin.000001"
                   for pos in self.position_events)

    def test_binlog_signal(self):
        assert all(isinstance(l[0], row_event.RowsEvent)
                   for l in self.binlog_events)
        assert all(isinstance(l[1], pymysqlreplication.BinLogStreamReader)
                   for l in self.binlog_events)
        assert len(self.binlog_events) == 7

    def test_rows_inserted_signal(self):
        assert len(self.rows_inserted_events) == 2

        # INSERT INTO tbl0 (data) VALUES ('a');
        rows0, meta0 = self.rows_inserted_events[0]
        assert len(rows0) == 1
        row00 = rows0[0]
        self.assertDictEqual(row00, {
            'keys': {u'id': 1},
            'values': {u'id': 1, u'data': u'a'}
        })
        self._assert_meta(meta0, 'insert')

        # INSERT INTO tbl0 (data) VALUES ('b'), ('c'), ('d');
        rows1, meta1 = self.rows_inserted_events[1]
        assert len(rows1) == 3
        for i, ch in zip((0, 1, 2), (u'b', u'c', u'd')):
            row = rows1[i]
            _id = i + 2
            self.assertDictEqual(row, {
                'keys': {u'id': _id},
                'values': {u'id': _id, u'data': ch}
            })
        self._assert_meta(meta1, 'insert')

    def test_rows_updated_signal(self):
        assert len(self.rows_updated_events) == 3

        # UPDATE tbl0 SET data = 'aa' WHERE id = 1;
        # UPDATE tbl0 SET data = 'bb' WHERE id = 2;
        for i, b_data, a_data in zip(
                (0, 1),
                (u'a', u'b'),
                (u'aa', u'bb')):
            rows, meta = self.rows_updated_events[i]
            assert len(rows) == 1
            self._assert_meta(meta, 'update')
            _id = i + 1
            self.assertDictEqual(rows[0], {
                'keys': {u'id': _id},
                'updated_values': {u'data': [b_data, a_data]},
                'values': {u'id': _id, u'data': a_data}
            })

        # UPDATE tbl0 SET data = 'cc' WHERE id != 1;
        rows, meta = self.rows_updated_events[2]
        assert len(rows) == 3
        self._assert_meta(meta, 'update')
        for i, b_data, a_data in zip(
                (0, 1, 2),
                (u'bb', u'c', u'd'),
                (u'cc', u'cc', u'cc')):
            _id = i + 2
            self.assertDictEqual(rows[i], {
                'keys': {u'id': _id},
                'updated_values': {u'data': [b_data, a_data]},
                'values': {u'id': _id, u'data': a_data}
            })

    def test_rows_deleted_signal(self):
        assert len(self.rows_deleted_events) == 2

        # DELETE FROM tbl0 WHERE id != 1;
        rows, meta = self.rows_deleted_events[0]
        assert len(rows) == 3
        self._assert_meta(meta, 'delete')
        for i, _id, a_data in zip(
                (0, 1, 2),
                (2, 3, 4),
                (u'cc', u'cc', u'cc')):
            _id = i + 2
            self.assertDictEqual(rows[i], {
                'keys': {u'id': _id},
                'values': {u'id': _id, u'data': a_data}
            })

        # DELETE FROM tbl0 WHERE id = 1;
        rows, meta = self.rows_deleted_events[1]
        assert len(rows) == 1
        self._assert_meta(meta, 'delete')
        self.assertDictEqual(rows[0], {
            'keys': {u'id': 1},
            'values': {u'id': 1, u'data': u'aa'}
        })

@pytest.fixture(scope='class')
def binlog_pos_storage_filename(request):
    # set a class attribute on the invoking test context
    import tempfile
    request.cls.binlog_pos_storage_filename = tempfile.mktemp(suffix='.binlog.pos')


@pytest.mark.usefixtures("binlog_pos_storage_filename")
class TestReplication(BaseTestCase):
    def _start_replication(self, resume_stream=False):
        start_replication(
            self.mysql_settings,
            binlog_pos_memory=(self.binlog_pos_storage_filename, 0.5),
            blocking=False,
            resume_stream=resume_stream,
        )

    def _make_more_binlog(self):
        # init mysql connection
        conn = pymysql.connect(**self.mysql_settings)
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
        _lf = self.position_events[0][0]
        while self.position_events:
            self.position_events.pop()

        self._make_more_binlog()

        # continue replicating
        self._start_replication(resume_stream=True)

        # the just received pos signals must be greater than last saved pos
        for lf, lp in self.position_events:
            assert lf == _lf
            assert lp > log_pos
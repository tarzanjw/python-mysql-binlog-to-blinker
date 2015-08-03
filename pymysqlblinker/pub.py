# -*- coding: utf-8 -*-
import logging
import random
from urlparse import urlparse
import datetime
import pymysqlreplication
from pymysqlreplication.row_event import (
    RowsEvent,
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from . import signals

__author__ = 'tarzan'
_logger = logging.getLogger(__name__)



def start_publishing(mysql_dsn, **kwargs):
    """Start MySQL row-based binlog events publishing.

    The additional kwargs will be passed to `BinLogStreamReader`.
    """
    # parse mysql settings
    parsed = urlparse(mysql_dsn)
    mysql_settings = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "passwd": parsed.password
    }
    kwargs.setdefault('server_id', random.randint(1000000000, 4294967295))
    kwargs.setdefault('freeze_schema', True)

    # connect to binlog stream
    stream = pymysqlreplication.BinLogStreamReader(
        mysql_settings,
        only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
        **kwargs
    )
    """:type list[RowsEvent]"""

    for event in stream:
        # ignore non row events
        if not isinstance(event, RowsEvent): continue;

        timestamp = datetime.datetime.fromtimestamp(event.timestamp)
        # _logger.debug('Got event %s(%s, %s) at %s'
        #               % (event.__class__.__name__, event.schema, event.table, timestamp))
        # do not know why the guys put this code here
        try:
            rows = event.rows
        except (UnicodeDecodeError, ValueError) as e:
            _logger.exception(e)
            continue

        if isinstance(event, WriteRowsEvent):
            action_signal = signals.write
        elif isinstance(event, UpdateRowsEvent):
            action_signal = signals.update
        elif isinstance(event, DeleteRowsEvent):
            action_signal = signals.delete
        else:
            _logger.critical('Unknown event class "%s"' % event.__class__.__name__)
            continue

        schema_signal = action_signal.__getattr__(event.schema)
        table_signal = schema_signal.__getattr__(event.table)
        row_signal = table_signal.row

        _logger.debug('Send event "%s" at %s' % (schema_signal.name, timestamp))
        schema_signal.send(event)

        _logger.debug('Send event "%s" at %s' % (table_signal.name, timestamp))
        table_signal.send(event)

        for row in rows:
            _logger.debug('Send event "%s" at %s' % (row_signal.name, timestamp))
            row_signal.send(row)

        signals.binlog_pos_signal.send((stream.log_file, stream.log_pos))
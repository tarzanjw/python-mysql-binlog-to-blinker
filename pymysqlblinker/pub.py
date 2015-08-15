# -*- coding: utf-8 -*-
import logging
import random
try:
    # Python 3
    from urllib.parse import urlparse, parse_qsl
except ImportError:
    # Python 2
    from urlparse import urlparse, parse_qsl
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


def send_signals_for_event(event):
    """ Send signals that is corresponding binlog event
    :param RowsEvent event: the binlog event
    """
    # do not know why the guys put this code here
    schema = event.schema
    table = event.table
    if isinstance(event, WriteRowsEvent):
        binlog_sig = signals.binlog_write
        schema_sig = signals.schema_write(schema)
        table_sig = signals.table_write(schema, table)
        row_sig = signals.row_write(schema, table)
    elif isinstance(event, UpdateRowsEvent):
        binlog_sig = signals.binlog_update
        schema_sig = signals.schema_update(schema)
        table_sig = signals.table_update(schema, table)
        row_sig = signals.row_update(schema, table)
    elif isinstance(event, DeleteRowsEvent):
        binlog_sig = signals.binlog_delete
        schema_sig = signals.schema_delete(schema)
        table_sig = signals.table_delete(schema, table)
        row_sig = signals.row_delete(schema, table)
    else:
        _logger.critical('Unknown event class "%s"' %
                         event.__class__.__name__)
        return

    # send binlog signal
    _logger.debug('Send binlog signal "%s"' % binlog_sig.name)
    binlog_sig.send(event, schema=schema, table=table)

    # send schema signal
    _logger.debug('Send schema signal "%s"' % schema_sig.name)
    schema_sig.send(event, schema=schema, table=table)

    try:
        rows = event.rows
    except (UnicodeDecodeError, ValueError) as e:
        _logger.exception(e)
        return

    # send table signal
    _logger.debug('Send table signal "%s"' % table_sig.name)
    table_sig.send(rows, schema=schema, table=table)

    # send row signals
    for row in rows:
        _logger.debug('Send row signal "%s"' % (row_sig.name))
        row_sig.send(row, schema=schema, table=table)


def start_publishing(mysql_dsn, **kwargs):
    """Start MySQL row-based binlog events publishing.

    The additional kwargs will be passed to `BinLogStreamReader`.
    """
    # parse mysql settings
    _logger.info('Start publishing from %s' % mysql_dsn)
    parsed = urlparse(mysql_dsn)
    mysql_settings = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "passwd": parsed.password,
        "connect_timeout": kwargs.pop('connect_timeout', None)
    }
    mysql_settings.update(parse_qsl(parsed.query))
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
        if not isinstance(event, RowsEvent):
            continue

        send_signals_for_event(event)
        signals.binlog_pos_signal.send((stream.log_file, stream.log_pos))

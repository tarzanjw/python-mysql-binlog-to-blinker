# -*- coding: utf-8 -*-
import logging
import random
from six.moves.urllib.parse import urlparse
import six
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
    try:
        rows = event.rows
    except (UnicodeDecodeError, ValueError) as e:
        _logger.exception(e)
        return

    schema = event.schema
    table = event.table
    if isinstance(event, WriteRowsEvent):
        binlog_sig = signals.binlog_write(schema, table)
        rows_sig = signals.rows_write(schema, table)
        row_sig = signals.row_write(schema, table)
    elif isinstance(event, UpdateRowsEvent):
        binlog_sig = signals.binlog_update(schema, table)
        rows_sig = signals.rows_update(schema, table)
        row_sig = signals.row_update(schema, table)
    elif isinstance(event, DeleteRowsEvent):
        binlog_sig = signals.binlog_delete(schema, table)
        rows_sig = signals.rows_delete(schema, table)
        row_sig = signals.row_delete(schema, table)
    else:
        _logger.critical('Unknown event class "%s"' %
                         event.__class__.__name__)
        return

    # send binglog signal
    _logger.debug('Send binlog signal "%s"' % binlog_sig.name)
    binlog_sig.send(event)

    # send rows signal
    _logger.debug('Send rows signal "%s"' % rows_sig.name)
    rows_sig.send(rows)

    # send row signals
    for row in rows:
        _logger.debug('Send row signal "%s"' % (row_sig.name))
        row_sig.send(row)


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
        if not isinstance(event, RowsEvent):
            continue

        send_signals_for_event(event)
        signals.binlog_pos_signal.send((stream.log_file, stream.log_pos))

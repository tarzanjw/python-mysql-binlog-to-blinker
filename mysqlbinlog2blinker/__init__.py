# -*- coding: utf-8 -*-
import logging
import random

import pymysqlreplication
from pymysqlreplication import row_event
from mysqlbinlog2blinker import (
    _subscribers,
    binlog_pos_memory as _bpm,
    signals,
)


__author__ = 'tarzan'
_logger = logging.getLogger(__name__)


def start_publishing(mysql_settings, **kwargs):
    """Start publishing MySQL row-based binlog events to blinker signals

    Args:
        mysql_settings (dict): information to connect to mysql via pymysql
        **kwargs: The additional kwargs will be passed to
        :py:class:`pymysqlreplication.BinLogStreamReader`.
    """
    _logger.info('Start publishing from %s with:\n%s'
                 % (mysql_settings, kwargs))

    kwargs.setdefault('server_id', random.randint(1000000000, 4294967295))
    kwargs.setdefault('freeze_schema', True)

    # connect to binlog stream
    stream = pymysqlreplication.BinLogStreamReader(
        mysql_settings,
        only_events=[row_event.DeleteRowsEvent,
                     row_event.UpdateRowsEvent,
                     row_event.WriteRowsEvent],
        **kwargs
    )
    """:type list[RowsEvent]"""

    for event in stream:
        # ignore non row events
        if not isinstance(event, row_event.RowsEvent):
            continue

        _logger.debug('Send binlog signal "%s@%s.%s"' % (
            event.__class__.__name__,
            event.schema,
            event.table
        ))
        signals.binlog_signal.send(event, stream=stream)
        signals.binlog_position_signal.send((stream.log_file, stream.log_pos))


def start_replication(mysql_settings,
                      binlog_pos_memory=(None, 2),
                      **kwargs):
    """ Start replication on server specified by *mysql_settings*

    Args:
         mysql_settings (dict): mysql settings that is used to connect to
                                mysql via pymysql
        binlog_pos_memory (_bpm.BaseBinlogPosMemory):
            Binlog Position Memory, it should be an instance of subclass of
            :py:class:`_bpm.BaseBinlogPosMemory`.

            If a tuple (str, float) is passed, it will be initialize parameters
            for default :py:class:`_bpm.FileBasedBinlogPosMemory`. It the file-
            name is None, it will be *`cwd`\mysqlbinlog2blinker.binlog.pos*
        **kwargs: any arguments that are accepted by
            :py:class:`pymysqlreplication.BinLogStreamReader`'s constructor
    """
    if not isinstance(binlog_pos_memory, _bpm.BaseBinlogPosMemory):
        if not isinstance(binlog_pos_memory, (tuple, list)):
            raise ValueError('Invalid binlog position memory: %s'
                             % binlog_pos_memory)
        binlog_pos_memory = _bpm.FileBasedBinlogPosMemory(*binlog_pos_memory)

    mysql_settings.setdefault('connect_timeout', 5)
    kwargs.setdefault('blocking', True)
    kwargs.setdefault('resume_stream', True)

    with binlog_pos_memory:
        kwargs.setdefault('log_file', binlog_pos_memory.log_file)
        kwargs.setdefault('log_pos', binlog_pos_memory.log_pos)
        _logger.info('Start replication from %s with:\n%s'
                     % (mysql_settings, kwargs))

        start_publishing(mysql_settings, **kwargs)
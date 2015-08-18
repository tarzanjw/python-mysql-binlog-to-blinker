# -*- coding: utf-8 -*-
import logging
from .binlog_pos_memory import BinlogPosMemory
from .pub import start_publishing


__version__ = '1.2'
__author__ = 'tarzan'
_logger = logging.getLogger(__name__)


def start_replication(mysql_dsn,
                      binlog_pos_storage_filename=None,
                      binlog_pos_storage_interval=2,
                      connect_timeout=5,
                      **kwargs):
    """ Start replication at *mysql_dsn*.
    :param string mysql_dsn: mysql dsn
    :param string binlog_pos_storage_filename: file name to storage last binlog
    position. Default is *`cwd`\pymysqlblinker.binlog.pos*
    :param float binlog_pos_storage_interval: see :class:BinlogPosMemory,
    in second
    :param float connect_timeout: master mysql connection timeout. In second.
    :param kwargs: this will pass through to :func:start_publishing
    :return:
    """
    if not binlog_pos_storage_filename:
        import os
        binlog_pos_storage_filename = os.path.join(os.getcwd(),
                                                   'pymysqlblinker.binlog.pos')
    binlog_memory = BinlogPosMemory(
        binlog_pos_storage_filename,
        binlog_pos_storage_interval,
    )

    with binlog_memory:
        publishing_kwargs = {
            'blocking': True,
            'log_file': binlog_memory.log_file,
            'log_pos': binlog_memory.log_pos,
            'resume_stream': True,
        }
        publishing_kwargs.update(kwargs)
        _logger.info('Start replication from %s with:\n%s'
                     % (mysql_dsn, publishing_kwargs))
        pub.start_publishing(
            mysql_dsn,
            connect_timeout=connect_timeout,
            **publishing_kwargs
        )
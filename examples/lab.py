# -*- coding: utf-8 -*-
from __future__ import print_function
import logging
import pprint
import mysqlbinlog2blinker
import pymysqlreplication
from pymysqlreplication import row_event
from mysqlbinlog2blinker import signals


def setup_logging(level=None):
    import logging.config
    import logging
    import os

    _ini = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        'logging.ini',
    ))
    logging.config.fileConfig(_ini, disable_existing_loggers=False)
    if level:
        logging.root.setLevel(level)

setup_logging(logging.INFO)

__author__ = 'tarzan'
_logger = logging.getLogger(__name__)


def main():
    mysql_settings = {
        'host': 'localhost',
        'user': 'binlog_publisher',
        'passwd': 'EWwjGWf9U346',
        'port': 33060,
    }

    stream = pymysqlreplication.BinLogStreamReader(
        mysql_settings,
        only_events=[row_event.WriteRowsEvent,
                     row_event.UpdateRowsEvent,
                     row_event.DeleteRowsEvent],
        only_tables=['orders_new', ],
        server_id=10021,
    )
    stream = iter(stream)
    e = next(stream)  # type: row_event.RowsEvent

    print(e.rows)
    e.dump()

if __name__ == '__main__':
    main()

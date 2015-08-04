# -*- coding: utf-8 -*-
import logging
import pprint
from pymysqlblinker import pub, signals


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


@signals.on_binlog_write('testdb', 'tbl0')
@signals.on_binlog_update('testdb', 'tbl0')
@signals.on_binlog_delete('testdb', 'tbl0')
def on_binlog_event(e):
    print('#'*8, 'BINLOG EVENT SIGNAL', '#'*8)
    e.dump()


@signals.on_rows_write('testdb', 'tbl0')
@signals.on_rows_update('testdb', 'tbl0')
@signals.on_rows_delete('testdb', 'tbl0')
def on_rows_event(rows):
    print('#'*8, 'ROWS SIGNAL', '#'*8)
    pprint.pprint(rows)


@signals.on_row_write('testdb', 'tbl0')
@signals.on_row_update('testdb', 'tbl0')
@signals.on_row_delete('testdb', 'tbl0')
def on_row_event(row):
    print('#'*8, 'ROW SIGNAL', '#'*8)
    pprint.pprint(row)


def main():
    mysql_dsn = 'mysql+pymysql://root@127.0.0.1/'

    pub.start_publishing(mysql_dsn)


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
from __future__ import print_function
import logging
import pprint
import mysqlbinlog2blinker
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


n_event = 0


def on_binlog_event(e, stream):
    global n_event
    n_event += 1
    if n_event >= 20:
        raise SystemExit()


@signals.on_rows_updated
def on_rows_updated(table, rows, meta):
    from datetime import datetime
    for row in rows:
        print(datetime.fromtimestamp(meta['time']),
              row['values']['use_id'],
              row['values']['use_loginname'])
        if 'use_loginname' not in row['updated_values']:
            continue
        before, after = row['updated_values']['use_loginname']
        if not ('inoxtuananh' in before or 'inoxtuananh' in after):
            continue
        print('\n', '*' * 32, '\n')
        print(row['values']['use_id'], row['values']['use_loginname'])
        print(row['updated_values'])
        print('SOS ' * 64)


def main():
    only_tables = ['orders_new', ]
    only_tables = ['users', ]
    # for i in range(20):
    #     only_tables.append('orders_product_%d' % i)
    mysqlbinlog2blinker.start_replication(
        {
            'host': 'localhost',
            'user': 'binlog_publisher',
            'passwd': 'EWwjGWf9U346',
            'port': 33060,
        },
        only_tables=only_tables,
        blocking=False,
    )


if __name__ == '__main__':
    main()

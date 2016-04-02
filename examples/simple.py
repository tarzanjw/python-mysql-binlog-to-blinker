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


@signals.on_rows_updated
def on_rows_updated(table, rows, meta):
    print('ROWS UPDATED')
    print(table)
    print(meta)
    print(rows)


def main():
    mysqlbinlog2blinker.start_replication(
        {
            'host': 'localhost',
            'user': 'root',
            'port': 3306,
        },
    )


if __name__ == '__main__':
    main()

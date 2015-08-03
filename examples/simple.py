# -*- coding: utf-8 -*-
import logging
from pymysqlblinker import pub, signals
import datetime

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


def dump_subscriber(e):
    e.dump()


def main():
    mysql_dsn = 'mysql+pymysql://root@127.0.0.1/'

    signals.write.testdb.connect(dump_subscriber)

    pub.start_publishing(mysql_dsn)


if __name__ == '__main__':
    main()
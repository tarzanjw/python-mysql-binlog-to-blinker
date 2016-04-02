# -*- coding: utf-8 -*-
"""MySQL binlog to Blinker

This project listen on mysql binlog (as a slave) and send the binlog event to
blinker signal. It can remember last binlog position and work as a MySQL slave.
"""
import logging

__author__ = 'tarzan'
_logger = logging.getLogger(__name__)


# The package name, which is also the "UNIX name" for the project.
package = 'mysqlbinlog2blinker'
project = "MySQL binlog to Blinker signal"
project_no_spaces = project.replace(' ', '')
version = '0.1'
description = 'This project listen on mysql binlog (as a slave) and send the ' \
              'binlog event to blinker signal. It can remember last binlog ' \
              'position and work as a MySQL slave.'
authors = ['Tarzan', ]
authors_string = ', '.join(authors)
emails = ['hoc3010@gmail.com', ]
license = 'MIT'
copyright = '2016 ' + authors_string
url = 'https://github.com/tarzanjw/python-mysql-binlog-to-blinker'
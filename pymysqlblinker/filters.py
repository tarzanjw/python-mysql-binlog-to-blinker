# -*- coding: utf-8 -*-
"""
This module contains some filters. This is just an idea.

>>> from pymysqlblinker import signals, filters
>>>
>>> @signals.write.testdb.testtable.row.connect
... @filters.row_attrs(id=20)
... def subscriber(row):
...     print(row['id']) # print out 20 only
"""
import logging

__author__ = 'tarzan'
_logger = logging.getLogger(__name__)


def row_attrs(**attrs):
    """
    Just the position holding
    :param attrs:
    :return:
    """
    def decorator(fn):
        def wrapper(row):
            return fn(row)
        return wrapper
    return decorator

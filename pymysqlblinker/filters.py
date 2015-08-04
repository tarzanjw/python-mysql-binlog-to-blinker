# -*- coding: utf-8 -*-
"""
This module contains some filters. This is just an idea.

"""
import logging

__author__ = 'tarzan'
_logger = logging.getLogger(__name__)


def remove_row_prefix(pk_val, row):
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

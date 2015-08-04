# -*- coding: utf-8 -*-
import blinker

__author__ = 'tarzan'

_signal_namespace = blinker.Namespace()

binlog_pos_signal = _signal_namespace.signal('mysql_binlog_pos')


def _signal(action, schema, table, suffix=None):
    """ Get the signal for *action* on *schema.table* at *suffix*
    :rtype: blinker.NamedSignal

    >>> _signal('action', 'db0', None).name
    'action@db0'
    >>> _signal('action', 'db1', 'tbl12').name
    'action@db1.tbl12'
    >>> _signal('action', 'db1', 'tbl12', 'suffix').name
    'action@db1.tbl12#suffix'
    >>> _signal('action', 'db', None, 'suffix')
    Traceback (most recent call last):
     ...
    AssertionError: Can not provide suffix when table is empty
    """
    assert table or not suffix, 'Can not provide suffix when table is empty'
    sig_name = '%s@%s' % (action, schema)
    if table:
        sig_name = sig_name + '.' + table
        if suffix:
            sig_name = sig_name + '#' + suffix
    return _signal_namespace.signal(sig_name)


def binlog_write(schema, table):
    """ Get the signal for binlog write on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('write', schema, table)


def binlog_update(schema, table):
    """ Get the signal for binlog update on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('update', schema, table)


def binlog_delete(schema, table):
    """ Get the signal for binlog delete on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('delete', schema, table)


def rows_write(schema, table):
    """ Get the signal for rows write on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('write', schema, table, 'rows')


def rows_update(schema, table):
    """ Get the signal for rows update on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('update', schema, table, 'rows')


def rows_delete(schema, table):
    """ Get the signal for rows delete on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('delete', schema, table, 'rows')


def row_write(schema, table):
    """ Get the signal for single row write on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('write', schema, table, 'row')


def row_update(schema, table):
    """ Get the signal for single row update on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('update', schema, table, 'row')


def row_delete(schema, table):
    """ Get the signal for single row delete on schema.table
    :rtype: blinker.NamedSignal
    """
    return _signal('delete', schema, table, 'row')


def on_binlog_write(schema, table):
    """ Decorator, wrapper for binlog_write(schema, table).connect
    :return: Function as decorator
    """
    return binlog_write(schema, table).connect


def on_binlog_update(schema, table):
    """ Decorator, wrapper for binlog_update(schema, table).connect
    :return: Function as decorator
    """
    return binlog_update(schema, table).connect


def on_binlog_delete(schema, table):
    """ Decorator, wrapper for binlog_delete(schema, table).connect
    :return: Function as decorator
    """
    return binlog_delete(schema, table).connect


def on_rows_write(schema, table):
    """ Decorator, wrapper for rows_write(schema, table).connect
    :return: Function as decorator
    """
    return rows_write(schema, table).connect


def on_rows_update(schema, table):
    """ Decorator, wrapper for rows_update(schema, table).connect
    :return: Function as decorator
    """
    return rows_update(schema, table).connect


def on_rows_delete(schema, table):
    """ Decorator, wrapper for rows_delete(schema, table).connect
    :return: Function as decorator
    """
    return rows_delete(schema, table).connect


def on_row_write(schema, table):
    """ Decorator, wrapper for row_write(schema, table).connect
    :return: Function as decorator
    """
    return row_write(schema, table).connect


def on_row_update(schema, table):
    """ Decorator, wrapper for row_update(schema, table).connect
    :return: Function as decorator
    """
    return row_update(schema, table).connect


def on_row_delete(schema, table):
    """ Decorator, wrapper for row_delete(schema, table).connect
    :return: Function as decorator
    """
    return row_delete(schema, table).connect

# -*- coding: utf-8 -*-
import blinker

__author__ = 'tarzan'

_signals = blinker.Namespace()

binlog_pos_signal = _signals.signal('mysql_binlog_pos')

#################
# WRITE SIGNALS #
#################

# The signal for binlog write
binlog_write = _signals.signal('write')


def schema_write(schema):
    """ Return the write signal on *schema*.
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('write@%s' % schema)


def table_write(schema, table):
    """ Return the write signal on *schema.table*
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('write@%s.%s' % (schema, table))


def row_write(schema, table):
    """ Return the write signal for a row on *schema.table*
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('write@%s.%s#row' % (schema, table))


# Decorator, wrapper for binlog_write.connect
on_binlog_write = binlog_write.connect


def on_schema_write(schema):
    """ Decorator, wrapper for schema_write(schema, table).connect
    :return: Function as decorator
    """
    return schema_write(schema).connect


def on_table_write(schema, table):
    """ Decorator, wrapper for table_write(schema, table).connect
    :return: Function as decorator
    """
    return table_write(schema, table).connect


def on_row_write(schema, table):
    """ Decorator, wrapper for row_write(schema, table).connect
    :return: Function as decorator
    """
    return row_write(schema, table).connect


#################
# UPDATE SIGNALS #
#################

# The signal for binlog update
binlog_update = _signals.signal('update')


def schema_update(schema):
    """ Return the update signal on *schema*.
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('update@%s' % schema)


def table_update(schema, table):
    """ Return the update signal on *schema.table*
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('update@%s.%s' % (schema, table))


def row_update(schema, table):
    """ Return the update signal for a row on *schema.table*
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('update@%s.%s#row' % (schema, table))


# Decorator, wrapper for binlog_update.connect
on_binlog_update = binlog_update.connect


def on_schema_update(schema):
    """ Decorator, wrapper for schema_update(schema, table).connect
    :return: Function as decorator
    """
    return schema_update(schema).connect


def on_table_update(schema, table):
    """ Decorator, wrapper for table_update(schema, table).connect
    :return: Function as decorator
    """
    return table_update(schema, table).connect


def on_row_update(schema, table):
    """ Decorator, wrapper for row_update(schema, table).connect
    :return: Function as decorator
    """
    return row_update(schema, table).connect


#################
# DELETE SIGNALS #
#################

# The signal for binlog delete
binlog_delete = _signals.signal('delete')


def schema_delete(schema):
    """ Return the delete signal on *schema*.
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('delete@%s' % schema)


def table_delete(schema, table):
    """ Return the delete signal on *schema.table*
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('delete@%s.%s' % (schema, table))


def row_delete(schema, table):
    """ Return the delete signal for a row on *schema.table*
        Multi calls with same args will return same signal.
    :rtype: blinker.NamedSignal
    """
    return _signals.signal('delete@%s.%s#row' % (schema, table))


# Decorator, wrapper for binlog_delete.connect
on_binlog_delete = binlog_delete.connect


def on_schema_delete(schema):
    """ Decorator, wrapper for schema_delete(schema, table).connect
    :return: Function as decorator
    """
    return schema_delete(schema).connect


def on_table_delete(schema, table):
    """ Decorator, wrapper for table_delete(schema, table).connect
    :return: Function as decorator
    """
    return table_delete(schema, table).connect


def on_row_delete(schema, table):
    """ Decorator, wrapper for row_delete(schema, table).connect
    :return: Function as decorator
    """
    return row_delete(schema, table).connect

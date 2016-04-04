# -*- coding: utf-8 -*-
""" This module describes the signals that are used on this package.

There are 3 kinds of signal:

**Position signal**

Will be sent after each event come to notify current binlog's position.

    def subscriber((log_file, log_pos):
        pass

**Binlog signal**

Will be sent for each binlog event, it could be Update/Write/Delete event.

    def subscriber(event, stream):
        pass

**Rows signals** : *the most important ones*

They will be sent on each binlog event, after convert event into a dict.

    def subscriber(table_name, rows, meta):
        pass

Where table_name if a string in form of schema.table. It is also blinker's
sender, so if you want to subscribe for one table only, just specify it via
sender argument of *connect* method.

There are 3 signals: rows_inserted, rows_updated, rows_deleted
"""
import blinker

__author__ = 'tarzan'


_signals = blinker.Namespace()


# position signal
binlog_position_signal = _signals.signal(
    'binlog_position'
)
""":type: blinker.NamedSignal"""

# fired on each RowsEvent come
# def subscriber(event, stream)
binlog_signal = _signals.signal(
    'event',
    doc='fired on each RowsEvent come',
)
""":type: blinker.NamedSignal"""
on_binlog = binlog_signal.connect

# def subscriber(table_name, rows, meta)
rows_inserted = _signals.signal(
    'rows_inserted',
    doc='fired on each WriteRowsEvent come, after convert data into dict',
)
""":type: blinker.NamedSignal"""
on_rows_inserted = rows_inserted.connect

# def subscriber(table_name, rows, meta)
rows_updated = _signals.signal(
    'rows_updated',
    doc='fired on each UpdateRowsEvent come, after convert data into dict',
)
""":type: blinker.NamedSignal"""
on_rows_updated = rows_updated.connect

# def subscriber(table_name, rows, meta)
rows_deleted = _signals.signal(
    'rows_deleted',
    doc='fired on each DeleteRowsEvent come, after convert data into dict',
)
""":type: blinker.NamedSignal"""
on_rows_deleted = rows_deleted.connect

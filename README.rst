Python MySQL Replication Blinker
================================

Features
--------

This package uses
`mysql-replication <https://github.com/noplay/python-mysql-replication>`__
to read events from MySQL's binlog and send to blinker's signal.

-  binlog action level
-  rows level

It will send RowsEvent only.

Whenever a binlog event come, it will be dispatched into some signals:

#. binlog\_position\_signal: 1 signal for the binlog current position
#. binlog\_signal: 1 signal for the binlog event.
#. rows\_signal: 1 signal for event's rows. 1 signal per row.


Signals
-------

There are 5 signals:

1. `binlog_position_signal`: sent whenever binlog event come to notify the
   current position of binlog stream
2. `binlog_signal`: sent whenever binlog event come to notify the binlog event
3. `rows_inserted_signal`, `rows_updated_signal`, `rows_deleted_signal`: sent
   on the event as their name


Connect to signals
------------------

To connect to a signal, you can use the signal instance or a decorator.

Suppose that you need to connect to write signal on table
*db0.table1*:

    .. code-block:: python

        from mysqlbinlog2blinker import signals

        @signal.rows_updated.connect
        def on_rows_updated_signal(table_name, rows, meta):
            pass

        @signal.binlog_signal.connect
        def on_binlog_signal(event, stream):
            pass

Signal publishing
-----------------

To start publishing signals

    .. code-block:: python

        from pymysqlblinker import start_publishing

        start_publishing(
            {
                'host': 'localohst',
                'user': 'root',
            },
        )

Replication
-----------

This package support a method to replicate from mysql database. It
operates by keep memory at last binlog position. By default, it save to a file.

To make it, call:

    .. code-block:: python

        from pymysqlblinker import start_replication

        start_replication(
            {
                'host': 'localohst',
                'user': 'root',
            },
            ('/path/to/file/that/remember/binlog/position', 2),
        )

Change logs
-----------

0.1
~~~

- First version
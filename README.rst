Python MySQL Replication Blinker
================================

Features
--------

| This package uses
`mysql-replication <https://github.com/noplay/python-mysql-replication>`__
to read
| events from MySQL's binlog and send to blinker's signal.

-  binlog action level
-  schema level
-  table level
-  row level

It will send RowsEvent only.

Whenever a binlog event come, it will be dispatched into some signals:

#. binlog\_signal: 1 signal for the binlog event.
#. schema\_signal: 1 signal for the event's schema
#. table\_signal: 1 signal for the event's table.
#. row\_signal: 1+ signals for event's rows. 1 signal per row.

Signals
-------

::

    binlog event -> binlog signal -> schema signal -> table signal --> row signals

So, suppose that an event come with schema=foo, table=bar and it updated
2 rows. Those signal will be sent:

+-----------------+--------------------------+------------------------------+
| signal          | signal name              | sender                       |
+=================+==========================+==============================+
| binlog signal   | ``update``               | event (RowsEvent)            |
+-----------------+--------------------------+------------------------------+
| schema signal   | ``update@foo``           | event (RowsEvent)            |
+-----------------+--------------------------+------------------------------+
| table signal    | ``update@foo.bar``       | event.rows (list of array)   |
+-----------------+--------------------------+------------------------------+
| row signal      | ``update@foo.bar#row``   | row1 (array)                 |
+-----------------+--------------------------+------------------------------+
| row signal      | ``update@foo.bar#row``   | row2 object (array)          |
+-----------------+--------------------------+------------------------------+

Connect to signals
------------------

To connect to a signal, you can use the signal instance or a decorator.

Suppose that you need to connect to write signal on table *db0.table1*:

::

    from pymysqlblinker import signals

    tbl1_signal = signals.table_write('db0', 'table1')

    def subscriber1(rows, schema, table):
        pass

    # use connect function
    tbl1_signal.connect(subscriber1)

    # or use decorator
    @signals.on_table_write('db0', 'table1')
    def subscriber1(rows, schema, table):
        pass

Change logs
-----------

1.1
~~~

::

    * Add connect_timeout argument to pub.start_publishing


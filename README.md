Python MySQL Replication Blinker
================================

    Test travis-ci build

> Underconstruction 

This package uses [mysql-replication](https://github.com/noplay/python-mysql-replication) to read
events from MySQL binlog and send to blinker's signal.

It will send RowsEvent only.

Whenever a binlog event come, it will be dispatched into some signals:

1. 1 signal for the event's database.
2. 1 signal for the event's table.
3. 1+ signals for event's rows. 1 signal per row.


Signals
-------

### Signal name

The signal name has formation as :

    action@schema.[table][#row]

So, suppose that an event come with schema=foo, table=bar and it updated 2 rows. Those signal will be sent:


|    signal name     |    sender    |
| ------------------ | ------------ |
| update@foo         | event object |
| update@foo.bar     | event object |
| update@foo.bar#row | row1 object  |
| update@foo.bar#row | row2 object  |

# -*- coding: utf-8 -*-
import blinker

__author__ = 'tarzan'

_signals = {}


def signal(name, doc=None):
    """Return the :class:`NamedSignal` *name*, creating it if required.

    Repeated calls to this function will return the same signal object.

    >>> xx = signal('xxx')
    >>> _signals['xxx'] is xx
    True
    """
    global _signals
    try:
        return _signals[name]
    except KeyError:
        return _signals.setdefault(name, NamedSignal(name, doc))


class NamedSignal(blinker.NamedSignal):

    def __init__(self, name, doc=None):
        super(NamedSignal, self).__init__(name, doc)
        self.__sub_signals__ = {}

    def schema_(self, name):
        """
        Get signal for the schema that belongs to current scope
        :param string name: schema's name
        :return:
        :rtype: NamedSignal

        >>> sig = NamedSignal('action')
        >>> schema_sig = sig.schema_('database')
        >>> schema_sig.name
        'action@database'
        """
        return NamedSignal(self.name + '@' + name)

    def table_(self, name):
        """
        Get signal for the table that belongs to current scope
        :param string name: table's name
        :return:
        :rtype: NamedSignal

        >>> sig = NamedSignal('action')
        >>> schema_sig = sig.schema_('database')
        >>> table_sig = schema_sig.table_('table')
        >>> table_sig.name
        'action@database.table'
        """
        return NamedSignal(self.name + '.' + name)

    def __get_next_signal__(self, name):
        if name in self.__sub_signals__:
            return self.__sub_signals__[name]

        if '@' not in self.name:
            sig_name = self.name + '@' + name
        elif '.' not in self.name:
            sig_name = self.name + '.' + name
        else:
            assert name == 'row', 'After table, onle "row" is allowed'
            sig_name = self.name + '#row'
        return self.__sub_signals__.setdefault(name, signal(sig_name, '%s signal' % sig_name))

    def __getattr__(self, item):
        """
        Call :method:`__get_next_signal__` to get returned value If the attr *item* does not exist

        >>> a = NamedSignal('action')
        >>> type(a.send)
        <type 'instancemethod'>
        >>> a.test_database.name
        'action@test_database'
        >>> a.db0.tbl1.name
        'action@db0.tbl1'
        >>> a.db1.tbl2.row.name
        'action@db1.tbl2#row'
        """
        return self.__dict__.setdefault(item, self.__get_next_signal__(item))

    def __getitem__(self, item):
        """
        Call :method:`__get_next_signal__` to get returned value If the attr *item* does not exist

        >>> a = NamedSignal('action')
        >>> a['test_database'].name
        'action@test_database'
        >>> a['db0']['tbl1'].name
        'action@db0.tbl1'
        >>> a['db1']['tbl2']['row'].name
        'action@db1.tbl2#row'
        """
        return self.__get_next_signal__(item)


binlog_pos_signal = signal('mysql_binlog_pos')
write = signal('write')
update = signal('update')
delete = signal('delete')
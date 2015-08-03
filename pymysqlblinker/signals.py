# -*- coding: utf-8 -*-
import blinker

__author__ = 'tarzan'

_signals = {}


def signal(name, doc=None):
    """Return the :class:`NamedSignal` *name*, creating it if required.

    Repeated calls to this function will return the same signal object.
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
        return self.__sub_signals__.setdefault(
            name,
            signal(sig_name, '%s signal' % sig_name)
        )

    def __getattr__(self, item):
        """
        Call :method:`__get_next_signal__` to get returned value If the attr
        *item* does not exist
        """
        return self.__dict__.setdefault(item, self.__get_next_signal__(item))

    def __getitem__(self, item):
        """
        Call :method:`__get_next_signal__` to get returned value If the attr
        *item* does not exist

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

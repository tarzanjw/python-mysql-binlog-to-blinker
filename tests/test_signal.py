# -*- coding: utf-8 -*-
import logging
from pymysqlblinker import signals
import blinker

__author__ = 'tarzan'
_logger = logging.getLogger(__name__)


def test_signal_func():
    xx = signals.signal('xxx')
    assert signals._signals['xxx'] is xx


def test__getattr__():
    a = signals.NamedSignal('action')
    assert isinstance(a, blinker.Signal)
    assert a.test_database.name == 'action@test_database'
    assert a.db0.tbl1.name == 'action@db0.tbl1'
    assert a.db1.tbl2.row.name == 'action@db1.tbl2#row'

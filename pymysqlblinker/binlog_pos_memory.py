# -*- coding: utf-8 -*-
import logging
import sys
import threading
from pymysqlblinker import signals

__author__ = 'tarzan'
_logger = logging.getLogger(__name__)


class BinlogPosMemory(object):
    """
    This class will receive binlog position signal from pymysqlblinker and store
    it into a file.

    It will subscribe to pymysqlblinker.signals.binlog_pos_signal and store the
    position into memory.
    A thread will run and store this position into a file each interval.
    The position in file will be read the the memory start.
    """
    def __init__(self, pos_filename, interval=2):
        """ Create instance of BinlogPosMemory
        :param string pos_filename: position storage file
        :param float interval: the interval in second
        """
        self.pos_storage_filename = pos_filename
        assert self.pos_storage_filename
        self.interval = interval

        self._log_file = None
        self._log_pos = None
        self._pos_changed = False

        self.save_log_pos_thread_stop_flag = threading.Event()
        self.save_log_pos_thread = \
            threading.Thread(target=self._save_log_pos_thread_runner)
        self.save_log_pos_thread.daemon = True

    def _save_log_pos_thread_runner(self):
        """ Save the position into file after every second
        :return:
        """
        _logger.debug('Start log pos saving thread')
        while not self.save_log_pos_thread_stop_flag.wait(self.interval):
            self._save_file_and_pos()
        _logger.debug('Finish log pos saving thread')

    @property
    def log_pos(self):
        return self._log_pos

    @property
    def log_file(self):
        return self._log_file

    def set_binlog_pos(self, log_file, log_pos):
        if (log_file != self._log_file) or (log_pos != self._log_pos):
            self._log_file, self._log_pos = log_file, log_pos
            self._pos_changed = True

    def on_binlog_pos_signal(self, pos):
        self.set_binlog_pos(*pos)

    def start(self):
        _logger.debug('Start binlog position memory at %s'
                      % self.pos_storage_filename)
        self._read_file_and_pos()
        signals.binlog_pos_signal.connect(self.on_binlog_pos_signal)
        self.save_log_pos_thread_stop_flag.clear()
        self.save_log_pos_thread.start()

    def stop(self):
        signals.binlog_pos_signal.disconnect(self.on_binlog_pos_signal)
        self.save_log_pos_thread_stop_flag.set()
        _logger.debug('Finish binlog position memory at %s'
                      % self.pos_storage_filename)
        # flushing current position into file
        self._save_file_and_pos()

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def _save_file_and_pos(self):
        """ Save current position into file
        """
        if not self._pos_changed:
            return
        with open(self.pos_storage_filename, 'w+') as f:
            _pos = '%s:%s' % (self._log_file, self._log_pos)
            _logger.debug('Saving position %s to file %s'
                          % (_pos, self.pos_storage_filename))
            f.write(_pos)
            self._pos_changed = False

    def _read_file_and_pos(self):
        """ Read last position from file, store as current position
        """
        try:
            with open(self.pos_storage_filename, 'r+') as f:
                _pos = f.read()
                _logger.debug('Got position "%s" from file %s'
                              % (_pos, self.pos_storage_filename))
                if not _pos:
                    return
                log_file, log_pos = _pos.split(':')
                try:
                    log_file = str(log_file)
                    log_pos = int(log_pos)
                except (ValueError, TypeError) as e:
                    _logger.critical('Can not read position: %s' % e)
                    sys.exit(13)
                self._log_file = log_file
                self._log_pos = log_pos
        except IOError as e:
            _logger.error(e)
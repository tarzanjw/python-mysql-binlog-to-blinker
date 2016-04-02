# coding=utf-8
import logging

from pymysqlreplication import row_event

from mysqlbinlog2blinker import signals

__author__ = 'Tarzan'
_logger = logging.getLogger(__name__)


def _get_updated_values(before_values, after_values):
    """ Get updated values from 2 dicts of values

    Args:
        before_values (dict): values before update
        after_values (dict): values after update

    Returns:
        dict: a diff dict with key is field key, value is tuple of
              (before_value, after_value)
    """
    assert before_values.keys() == after_values.keys()
    return dict([(k, [before_values[k], after_values[k]])
                 for k in before_values.keys()
                 if before_values[k] != after_values[k]])


def _convert_write_row(row):
    """ Convert a row for write/delete event """
    return row


def _convert_update_row(row):
    """ Convert a row for update event

    Args:
        row (dict): event row data
    """
    after_values = row['after_values']  # type: dict
    before_values = row['before_values']  # type: dict
    values = after_values
    return {
        'values': values,
        'updated_values': _get_updated_values(before_values, after_values)
    }


def _rows_event_to_dict(e, stream):
    """ Convert RowsEvent to a dict

    Args:
        e (pymysqlreplication.row_event.RowsEvent): the event
        stream (pymysqlreplication.BinLogStreamReader):
            the stream that yields event

    Returns:
        dict: event's data as a dict
    """
    pk_cols = e.primary_key if isinstance(e.primary_key, (list, tuple)) \
        else (e.primary_key, )

    if isinstance(e, row_event.UpdateRowsEvent):
        sig = signals.rows_updated
        action = 'update'
        row_converter = _convert_update_row
    elif isinstance(e, row_event.WriteRowsEvent):
        sig = signals.rows_inserted
        action = 'insert'
        row_converter = _convert_write_row
    elif isinstance(e, row_event.DeleteRowsEvent):
        sig = signals.rows_deleted
        action = 'delete'
        row_converter = _convert_write_row
    else:
        assert False, 'Invalid binlog event'

    meta = {
        'time': e.timestamp,
        'log_pos': stream.log_pos,
        'log_file': stream.log_file,
        'schema': e.schema,
        'table': e.table,
        'action': action,
    }
    rows = list(map(row_converter, e.rows))
    for row in rows:
        row['keys'] = {k: row['values'][k] for k in pk_cols}
    return rows, meta


@signals.on_binlog
def on_binlog(event, stream):
    """ Process on a binlog event

    1. Convert event instance into a dict
    2. Send corresponding schema/table/signals

    Args:
        event (pymysqlreplication.row_event.RowsEvent): the event
    """
    rows, meta = _rows_event_to_dict(event, stream)

    table_name = '%s.%s' % (meta['schema'], meta['table'])

    if meta['action'] == 'insert':
        sig = signals.rows_inserted
    elif meta['action'] == 'update':
        sig = signals.rows_updated
    elif meta['action'] == 'delete':
        sig = signals.rows_deleted
    else:
        raise RuntimeError('Invalid action "%s"' % meta['action'])

    sig.send(table_name, rows=rows, meta=meta)
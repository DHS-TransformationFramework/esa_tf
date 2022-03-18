import logging
import sys
import time

import dask.distributed

from . import __version__


class DaskLogHandler(logging.Handler, object):
    """
     custom log handler
    """

    def __init__(self):
        logging.Handler.__init__(self)
        self.dask_worker = dask.distributed.worker.get_worker()

    def emit(self, record):
        """
        Send  teg redcord to dask log_event
        """
        msg = self.format(record)
        order_id = dask.distributed.worker.thread_state.key
        self.dask_worker.log_event(order_id, msg)


class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    def filter(self, record):
        order_id = None
        try:
            order_id = dask.distributed.worker.thread_state.key
        except AttributeError:
            pass
        record.order_id = order_id
        record.tf_version = __version__
        return True


# FIXME: where should you configure the log handler in a dask distributed application?
def add_stderr_handlers(logger):
    filter = ContextFilter()
    logging_formatter = logging.Formatter(
        "esa_tf-%(tf_version)s - %(name)s - order_id %(order_id)s - %(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%d/%m/%Y %H:%M:%S",
    )
    logging.Formatter.converter = time.gmtime

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(logging_formatter)
    stream_handler.addFilter(filter)
    logger.addHandler(stream_handler)

    try:
        dask.distributed.worker.get_worker()
    except ValueError:
        pass
    else:
        dask_handler = DaskLogHandler()
        dask_handler.setFormatter(logging_formatter)
        dask_handler.addFilter(filter)
        logger.addHandler(dask_handler)


def logger_setup():
    rootlogger = logging.getLogger()
    rootlogger.setLevel(logging.INFO)
    rootlogger.propagate = True
    add_stderr_handlers(rootlogger)

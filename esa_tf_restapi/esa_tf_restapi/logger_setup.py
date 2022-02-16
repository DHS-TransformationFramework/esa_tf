import logging
import sys
import time

from . import __version__

class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """

    def filter(self, record):
        record.tf_version = __version__
        return True


def add_stderr_handlers(logger):
    filter = ContextFilter()
    logging_formatter = logging.Formatter(
        "esa_tf-%(tf_version)s - %(name)s - %(asctime)s.%(msecs)03d - %(levelname)s - %(message)s ",
        datefmt="%d/%m/%Y %H:%M:%S",
    )
    logging.Formatter.converter = time.gmtime

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(logging_formatter)
    stream_handler.addFilter(filter)
    logger.addHandler(stream_handler)


def logger_setup():
    rootlogger = logging.getLogger()
    rootlogger.setLevel(logging.INFO)
    rootlogger.propagate = True
    add_stderr_handlers(rootlogger)

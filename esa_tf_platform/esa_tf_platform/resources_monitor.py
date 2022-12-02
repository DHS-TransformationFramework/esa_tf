import datetime
import logging
import os
import threading
import time

from . import logger_setup

logger = logging.getLogger(__name__)


LOOP = {"status": True}

B_TO_GB = 9.313225746154785 * 1e-10

def format_processing_time(start_processing_time, stop_processing_time):
    processing_time_s = (stop_processing_time - start_processing_time).total_seconds()
    hours = processing_time_s // (60 * 60)
    minutes = processing_time_s // 60 - hours * 60
    seconds = processing_time_s - minutes * 60 - hours * 60 * 60
    return f"{hours}:{minutes}:{seconds}"



def compute_disk_usage(processing_dir):
    total_size_b = 0
    for path, _, filenames in os.walk(processing_dir):
        for filename in filenames:
            filepath = os.path.join(path, filename)
            total_size_b += os.path.getsize(filepath)

    total_size_gb = total_size_b * B_TO_GB
    return total_size_gb


def resources_monitor(
    stop_event: threading.Event,
    order_id: str,
    processing_dir: str,
    polling_time: int = 10,
) -> None:
    logger_setup.ORDER_ID = order_id
    logger.warning(f"resources monitor running", extra={"order_id": order_id})
    ram_usage = []
    cpu_usage = []
    disk_usage = []
    start_processing_time = datetime.datetime.now()
    while not stop_event.isSet():
        dir_size = compute_disk_usage(processing_dir)
        disk_usage.append(dir_size)

        logger.info(f"disk usage: {dir_size * B_TO_GB} GB", extra={"order_id": order_id})

        time.sleep(polling_time)

    stop_processing_time = datetime.datetime.now()
    processing_time = format_processing_time(
        start_processing_time=start_processing_time,
        stop_processing_time=stop_processing_time,
    )

    logger.info(f"-----------------------------------------------------------", extra={"order_id": order_id})
    logger.info(f"wall time: {processing_time}", extra={"order_id": order_id})
    peak_disk_usage = max(disk_usage)
    logger.info(f"peak disk usage: {peak_disk_usage:.2f} Gb", extra={"order_id": order_id})
    logger.info(f"------------------------------------------------------------", extra={"order_id": order_id})

    return processing_time


# def terminate_handler(signum, stack_frame):
#     logger.info(f"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx terminate")
#     LOOP["status"] = False


# signal.signal(signal.SIGTERM, terminate_handler)

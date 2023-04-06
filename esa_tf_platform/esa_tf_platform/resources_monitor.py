import datetime
import logging
import os
import threading
import time

import psutil

logger = logging.getLogger(__name__)
B_TO_GB = 9.313225746154785 * 1e-10


def update_disk_usage(disk_usage: list[float], processing_dir: str) -> float:
    total_size_b = 0
    for path, _, filenames in os.walk(processing_dir):
        for filename in filenames:
            filepath = os.path.join(path, filename)
            total_size_b += os.path.getsize(filepath)

    total_size_gb = total_size_b * B_TO_GB
    disk_usage.append(total_size_gb)


def update_cpu_time(cpu_times: dict[int, list[float]], process):

    children = process.children(recursive=True)
    for child in children + [process]:
        ct = child.cpu_times()
        if child.pid not in cpu_times:
            cpu_times[child.pid] = [ct.user + ct.system]
        else:
            cpu_times[child.pid].append(ct.user + ct.system)


def update_ram_usage(ram_usage: dict[int, list[float]], process):

    children = process.children(recursive=True)
    vms = 0
    for child in children + [process]:
        vms += child.memory_info().vms * B_TO_GB
    ram_usage.append(vms)


def compute_cpu_time(cpu_times):
    cpu_time = 0
    for t in cpu_times.values():
        cpu_time += t[-1] - t[0]
    return cpu_time


def resources_monitor(
    stop_event: threading.Event,
    order_id: str,
    process_pid: int,
    processing_dir: str,
    monitoring_polling_time_s: int = 20,
) -> None:
    logger.info(f"resources monitor running", extra={"order_id": order_id})

    disk_usage = []
    cpu_times = {}
    ram_usage = []

    process = psutil.Process(process_pid)
    start_processing_time = datetime.datetime.now()

    while not stop_event.isSet():
        update_cpu_time(cpu_times, process)
        update_disk_usage(disk_usage, processing_dir)
        update_ram_usage(ram_usage, process)

        # logger.debug(f"disk usage: {disk_usage[-1]} Gb", extra={"order_id": order_id})
        # logger.debug(f"ram usage: {ram_usage} Gb", extra={"order_id": order_id})
        # logger.debug(f"cpu times: {cpu_times}", extra={"order_id": order_id})
        time.sleep(monitoring_polling_time_s)

    update_cpu_time(cpu_times, process)
    update_disk_usage(disk_usage, processing_dir)
    update_ram_usage(ram_usage, process)

    stop_processing_time = datetime.datetime.now()

    processing_time = (stop_processing_time - start_processing_time).total_seconds()
    peak_disk_usage = max(disk_usage)
    peak_ram_usage = max(ram_usage)
    cpu_time = compute_cpu_time(cpu_times)

    logger.info(f"wall time: {processing_time: .2f} s", extra={"order_id": order_id})
    logger.info(
        f"peak disk usage: {peak_disk_usage:.2f} Gb", extra={"order_id": order_id}
    )
    logger.info(
        f"peak RAM usage: {peak_ram_usage:.2f} Gb", extra={"order_id": order_id}
    )
    logger.info(f"total CPU Time: {cpu_time: .2f} s", extra={"order_id": order_id})

    return processing_time

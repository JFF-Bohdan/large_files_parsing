import logging
import queue
import re
from collections import Counter
from multiprocessing import current_process

from dateutil.parser import parse
from mp_app_support_funcs.support_functions import GroupByIPAndDateKey, init_logger  # noqa


def process_data(
    input_tasks_queue,
    valid_ip_addresses_reg_exp,
    columns_to_indexes_mapping,
    global_total_rows_processed,
    periodic_print_interval,
    global_results_dict,
    global_results_dict_lock
    # partial_results_queue
):
    current_process_name = current_process().name

    valid_ip_address_matcher = re.compile(valid_ip_addresses_reg_exp)

    logger = init_logger(current_process_name, logging.DEBUG)
    logger.info("data processing started")

    rows_processed_in_process = 0
    partial_results = Counter()

    dst_column_index = columns_to_indexes_mapping.get("dst", None)
    timestamp_column_index = columns_to_indexes_mapping.get("timestamp", None)
    message_column_index = columns_to_indexes_mapping.get("message", None)

    while True:
        try:
            task_data = input_tasks_queue.get(block=True)
            if task_data is None:
                break

            for initial_line_index, row_to_process in task_data:
                ip_address = row_to_process[dst_column_index]
                ip_address = str(ip_address).strip()

                rows_processed_in_process += 1
                if not valid_ip_address_matcher.match(ip_address):
                    continue

                timestamp = row_to_process[timestamp_column_index]
                timestamp = parse(timestamp)
                key = GroupByIPAndDateKey(
                    ip=ip_address,
                    date=timestamp.date(),
                    violation_rule=row_to_process[message_column_index]
                )
                partial_results[key] += 1

                if periodic_print_interval and (rows_processed_in_process % periodic_print_interval == 0):
                    logger.info(
                        "processor[{}] processed records count: {}".format(
                            current_process_name,
                            rows_processed_in_process
                        )
                    )

        except queue.Empty:
            break

    global_total_rows_processed.value += rows_processed_in_process

    logger.info(
        "processor[{}] joining results: {}".format(
            current_process_name,
            len(partial_results)
        )
    )

    # for key, value in partial_results.items():
    #     result_item = (key, value)
    #     partial_results_queue.put(result_item)

    with global_results_dict_lock:
        for key, value in partial_results.items():
            current_value = global_results_dict.get(key, 0)
            global_results_dict[key] = current_value + value

    return True

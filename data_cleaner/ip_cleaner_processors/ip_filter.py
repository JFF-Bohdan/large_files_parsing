import ipaddress
import logging
import queue
import random
from functools import partial
from multiprocessing import current_process

from data_cleaner.support_functions.ip_cleaner_functions_and_consts import generate_forbidden_networks
from data_cleaner.support_functions.support_functions import init_logger


def fake_ip_generator_func(ip_address_to_map, global_mapped_list, mapped_ip_addresses_lock):
    with mapped_ip_addresses_lock:
        ret = global_mapped_list.get(ip_address_to_map, None)
        if ret:
            return ret

        mapped_ip_address = [
            random.randint(1, 254),
            random.randint(0, 254),
            random.randint(1, 254),
            random.randint(1, 254)
        ]
        mapped_ip_address = ".".join([str(item) for item in mapped_ip_address])

        global_mapped_list[ip_address_to_map] = mapped_ip_address
        return mapped_ip_address


def filter_ip_addresses(
        line_index,
        row_data,
        indexes_to_filter,
        forbidden_networks,
        mock_function,
        forbidden_cache,
        logger
):
    row_len = len(row_data)
    res = row_data

    items_filtered = 0
    for index_to_filter in indexes_to_filter:
        if index_to_filter >= row_len:
            continue

        item = str(res[index_to_filter]).strip()
        if not item:
            continue

        new_value = None
        is_forbidden = False
        if (forbidden_cache is not None) and (item in forbidden_cache):
            is_forbidden = True
            new_value = forbidden_cache[item]

        if not is_forbidden:
            try:
                ip_address = ipaddress.ip_address(item)
            except BaseException as e:
                logger.error("Error converting {} (line={}) to IP address: {}".format(item, line_index, e))
                raise

            is_forbidden = False
            for networks in forbidden_networks:
                if ip_address in networks:
                    is_forbidden = True
                    break

        if is_forbidden:
            if new_value is None:
                new_value = mock_function(item)
                if forbidden_cache is not None:
                    forbidden_cache[item] = new_value

            item = new_value
            items_filtered += 1

        res[index_to_filter] = item

    return res, items_filtered


def csv_filter_process(
        input_tasks_queue,
        networks_to_filter,
        indexes_to_filter,
        output_tasks_queue,
        global_mapped_ip_addresses,
        mapped_ip_addresses_lock,
        global_total_items_filtered,
        write_task_chunk_size,
        periodic_print_interval
):
    processed_rows = 0
    current_process_name = current_process().name

    forbidden_networks = generate_forbidden_networks(networks_to_filter)
    logger = init_logger(current_process_name, logging.DEBUG)
    logger.info("filter process started")

    local_mapped_cache = {}
    partial_fake_ip_generator_func = partial(
        fake_ip_generator_func,
        global_mapped_list=global_mapped_ip_addresses,
        mapped_ip_addresses_lock=mapped_ip_addresses_lock
    )

    items_filtered_in_process = 0
    while True:
        try:
            task_data = input_tasks_queue.get(block=True)
            if task_data is None:
                break

            write_tasks = list()
            for initial_line_index, row_to_process in task_data:
                result_item, items_filtered = filter_ip_addresses(
                    initial_line_index,
                    row_to_process,
                    indexes_to_filter,
                    forbidden_networks,
                    partial_fake_ip_generator_func,
                    local_mapped_cache,
                    logger
                )
                items_filtered_in_process += items_filtered

                write_tasks.append(result_item)
                if len(write_tasks) >= write_task_chunk_size:
                    output_tasks_queue.put(write_tasks)
                    write_tasks = list()

                processed_rows += 1
                if periodic_print_interval and (processed_rows % periodic_print_interval == 0):
                    logger.info("filter[{}] processed records count: {}".format(current_process_name, processed_rows))

            if write_tasks:
                output_tasks_queue.put(write_tasks)

        except queue.Empty:
            break

    logger.info("filter process finished (items_filtered_in_process: {})".format(items_filtered_in_process))
    global_total_items_filtered.value += items_filtered_in_process
    return True

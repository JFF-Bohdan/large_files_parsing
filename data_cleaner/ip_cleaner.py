import codecs
import copy
import csv
import ipaddress
import logging
import os
import random
import sys
import time
from typing import List, Tuple

import terminaltables

abs_file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(abs_file_dir)
sys.path.append(os.path.join(abs_file_dir, ".."))

from data_cleaner.support_functions.ip_cleaner_functions_and_consts import (produce_mapping_report,  # noqa
    parse_header_indexes, generate_forbidden_networks)
from data_cleaner.support_functions.support_functions import iterate_over_csv_input_data, init_logger  # noqa
from data_cleaner.support_functions.ip_cleaner_command_line_args_initializer import parse_command_line  # noqa


def fake_ip_generator_func(_):
    res = [random.randint(1, 254), random.randint(0, 254), random.randint(1, 254), random.randint(1, 254)]
    return ".".join([str(item) for item in res])


def filter_row_data(
    item_row,
    indexes_to_filter,
    forbidden_networks,
    mock_function,
    logger,
    forbidden_cache=None,
    line_index=None
) -> Tuple[List, int]:
    row_len = len(item_row)

    items_filtered = 0
    res = copy.deepcopy(item_row)
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


def main():
    args = parse_command_line()

    logger = init_logger(log_name=__name__, log_level=logging.DEBUG)

    logger.info("application started")
    if None in [args.input, args.output]:
        logger.error("Please specify both paths: input and output")
        return -1

    if not os.path.exists(args.input):
        logger.error("file '{}' does not exists".format(args.input))
        return -1

    ip_columns = [str(item).strip() for item in str(args.ip_columns).split(",") if str(item).strip()]
    if not ip_columns:
        logger.error("No ip columns specified to be filtered")
        return -1

    forbidden_networks = generate_forbidden_networks()

    logger.info("going to process file '{}'".format(args.input))
    logger.info("going to generate results into '{}'".format(args.output))

    tm_begin = time.time()
    indexes_to_filter = []

    total_records_processed = 0
    total_items_filtered = 0
    forbidden_items_cache = dict()
    total_size_processed = 0
    total_file_size = os.path.getsize(args.input)

    with codecs.open(args.output, "w", "utf-8") as output_file:
        data_writer = csv.writer(output_file, delimiter=args.delimiter, quotechar=args.quote_char)

        for item_index, item_row in enumerate(iterate_over_csv_input_data(args.input, args.delimiter, args.quote_char)):
            total_size_processed += len(str(item_row)) + 2

            if not item_index:
                indexes_to_filter = parse_header_indexes(item_row, ip_columns)
                data_writer.writerow(item_row)
                continue

            if(
                total_records_processed and
                args.periodic_interval and
                (total_records_processed % args.periodic_interval == 0)
            ):
                time_spent = round(time.time() - tm_begin, 3)
                rows_per_second = None
                if time_spent > 0:
                    rows_per_second = round((total_records_processed / time_spent), 2)

                if not args.output_limit:
                    processed_percs = round((total_size_processed / total_file_size) * 100, 2)
                else:
                    processed_percs = round((total_records_processed / args.output_limit) * 100, 2)

                logger.info(
                    "{} records processed [RPS: {} processed: {}%]".format(
                        total_records_processed,
                        rows_per_second,
                        processed_percs
                    )
                )

            item_row, items_filtered = filter_row_data(
                item_row,
                indexes_to_filter,
                forbidden_networks,
                fake_ip_generator_func,
                logger,
                forbidden_cache=forbidden_items_cache,
                line_index=item_index
            )

            data_writer.writerow(item_row)
            total_records_processed += 1
            total_items_filtered += items_filtered

            if args.output_limit and (total_records_processed >= args.output_limit):
                break

    if args.produce_report:
        produce_mapping_report(logger, forbidden_items_cache)

    time_elapsed = round(time.time() - tm_begin, 3)
    if time_elapsed > 0:
        rows_per_second = round((total_records_processed / time_elapsed), 2)
    else:
        rows_per_second = 0

    table_data = [
        ["Item", "Value"],
        ["Total records Processed", total_records_processed],
        ["Items filtered", total_items_filtered],
        ["Rows per second", rows_per_second],
        ["Mapped IP addresses count", len(forbidden_items_cache)],
        ["Time elapsed", time_elapsed]
    ]

    table = terminaltables.AsciiTable(table_data)
    logger.info("\n{}".format(table.table))
    logger.info("application finished in {}".format(time_elapsed))
    return 0


if __name__ == "__main__":
    res = main()
    exit(res)

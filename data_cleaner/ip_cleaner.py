import argparse
import codecs
import copy
import csv
import ipaddress
import logging
import os
import random
import time
from typing import List, Tuple

import terminaltables

FORBIDDEN_NETWORKS = [
    "194.24.186.0/23",
    "185.121.116.0/24",
    "185.121.118.0/24",
    "185.121.117.0/24",
    "93.158.223.224/27",
    "5.178.64.32/27",
    "185.121.119.0/24"
]

DEFAULT_COLUMNS_TO_BE_CLEANED = "dst,ip"


def fake_ip_generator_func(_):
    res = [random.randint(1, 254), random.randint(0, 254), random.randint(1, 254), random.randint(1, 254)]
    return ".".join([str(item) for item in res])


def init_logger(log_name=__name__, log_level=logging.DEBUG):
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.propagate = False

    return logger


def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        metavar="FILE",
        help="input file path"
    )

    parser.add_argument(
        "--output",
        metavar="FILE",
        help="output file path"
    )

    parser.add_argument(
        "--ip_columns",
        action="store",
        help="RegExp for IP addresses validation",
        default=DEFAULT_COLUMNS_TO_BE_CLEANED
    )

    parser.add_argument(
        "--delimiter",
        action="store",
        help="CSV delimiter",
        default=","
    )

    parser.add_argument(
        "--quote_char",
        action="store",
        help="CSV quote char",
        default="\""
    )

    return parser.parse_args()


def iterate_over_input_data(input_file_name, delimiter, quote_char):
    with codecs.open(input_file_name, "r", "utf-8") as input_csvfile:
        data_reader = csv.reader(input_csvfile, delimiter=delimiter, quotechar=quote_char)
        for row_dict in data_reader:
            yield row_dict


def parse_header_indexes(item_row, ip_columns):
    res = []

    ip_columns = list(set(ip_columns))
    for column_index, column_name in enumerate(item_row):
        if column_name not in ip_columns:
            continue

        res.append(column_index)

    return res


def filter_row_data(
    item_row,
    indexes_to_filter,
    forbidden_networks,
    mock_function,
    forbidden_cache=None,
    logger=None
) -> Tuple[List, int]:
    row_len = len(item_row)

    items_filtered = 0
    res = copy.deepcopy(item_row)
    for index_to_filter in indexes_to_filter:
        if index_to_filter >= row_len:
            continue

        item = str(res[index_to_filter]).strip()

        new_value = None
        is_forbidden = False
        if (forbidden_cache is not None) and (item in forbidden_cache):
            is_forbidden = True
            new_value = forbidden_cache[item]

        if not is_forbidden:
            ip_address = ipaddress.ip_address(item)

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

    forbidden_networks = []
    for ip_address in FORBIDDEN_NETWORKS:
        forbidden_networks.append(ipaddress.ip_network(ip_address))

    logger.info("going to process file '{}'".format(args.input))
    logger.info("going to generate results into '{}'".format(args.output))

    tm_begin = time.time()
    indexes_to_filter = []

    records_processed = 0
    total_items_filtered = 0
    forbidden_items_cache = dict()
    with codecs.open(args.output, "w", "utf-8") as output_file:
        data_writer = csv.writer(output_file, delimiter=args.delimiter, quotechar=args.quote_char)

        for item_index, item_row in enumerate(iterate_over_input_data(args.input, args.delimiter, args.quote_char)):
            if not item_index:
                indexes_to_filter = parse_header_indexes(item_row, ip_columns)
                data_writer.writerow(item_row)
                continue

            item_row, items_filtered = filter_row_data(
                item_row,
                indexes_to_filter,
                forbidden_networks,
                fake_ip_generator_func,
                forbidden_cache=forbidden_items_cache,
                logger=logger
            )

            data_writer.writerow(item_row)
            records_processed += 1
            total_items_filtered += items_filtered

    table_data = [
        ["IP Address", "Mocked IP Address"]
    ]
    for key, value in forbidden_items_cache.items():
        table_data.append([key, value])
    table = terminaltables.AsciiTable(table_data)
    logger.info("Mocked IP Addresses:\n{}".format(table.table))

    time_elapsed = round(time.time() - tm_begin, 3)
    table_data = [
        ["Item", "Value"],
        ["Records Processed", records_processed],
        ["Items filtered", total_items_filtered],
        ["Time elapsed", time_elapsed]
    ]

    table = terminaltables.AsciiTable(table_data)
    logger.info("\n{}".format(table.table))
    logger.info("application finished in {}".format(time_elapsed))
    return 0


if __name__ == "__main__":
    res = main()
    exit(res)
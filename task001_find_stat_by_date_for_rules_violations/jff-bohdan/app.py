import argparse
import csv
import logging
import os
import re
import time
from collections import Counter, namedtuple

from dateutil.parser import parse

import terminaltables

PERIODIC_PRINT_INTERVAL = 10000
DEFAULT_IP_ADDRESSES_MATCHER_REG_EXP = "192\.168\..*"

GroupByIPAndDateKey = namedtuple("GroupByIPAndDateKey", ["ip", "date", "violation_rule"])


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
        "--ip_address_mask",
        action="store",
        help="RegExp for IP addresses validation",
        default=DEFAULT_IP_ADDRESSES_MATCHER_REG_EXP
    )

    return parser.parse_args()


def iterate_over_input_data(input_file_name):
    with open(input_file_name, "r") as csvfile:
        data_reader = csv.DictReader(csvfile, delimiter=",", quotechar="\"")
        for row_dict in data_reader:
            yield row_dict


def generate_output_grouped_by_date_ip_and_violated_rule(input_file_name, is_ip_matches_func, logger, output_file_name):
    result_counter = Counter()

    total_rows_processed = 0
    for index, data_dict in enumerate(iterate_over_input_data(input_file_name)):
        ip_address = data_dict["dst"]
        ip_address = str(ip_address).strip()

        if index and (index % PERIODIC_PRINT_INTERVAL == 0):
            logger.info("{} records processed".format(index))

        if not is_ip_matches_func(ip_address):
            logger.debug("Skipping IP '{}'".format(ip_address))
            continue

        timestamp = data_dict["timestamp"]
        timestamp = parse(timestamp)
        key = GroupByIPAndDateKey(ip=ip_address, date=timestamp.date(), violation_rule=data_dict["message"])
        result_counter[key] += 1

        total_rows_processed = index

    logger.info("saving output data")
    dialect = csv.excel
    dialect.delimiter = ";"
    header = ["Date", "IP", "Violation Rule", "Violations count"]

    added_rows_count = 0
    with open(output_file_name, "w", newline="") as output_csv_file:
        writer = csv.writer(output_csv_file, dialect=dialect)
        writer.writerow(header)

        keys = result_counter.keys()
        keys = sorted(keys, key=(lambda x: x.date))

        for key in keys:
            violations_count = result_counter[key]
            writer.writerow([key.date, key.ip, key.violation_rule, violations_count])
            added_rows_count += 1

    logger.info("data processing finished")
    return total_rows_processed, added_rows_count


def main():
    args = parse_command_line()

    logger = init_logger(log_name="log_parser", log_level=logging.INFO)

    logger.info("application started")
    if None in [args.input, args.output]:
        logger.error("Please specify both paths: input and output")
        return -1

    if not os.path.exists(args.input):
        logger.error("file '{}' does not exists".format(args.input))
        return -1

    valid_ip_address_matcher = re.compile(args.ip_address_mask)
    valid_ip_address_matcher_func = (lambda ip_address: valid_ip_address_matcher.match(ip_address))

    tm_begin = time.time()

    total_rows_processed, added_rows_count = generate_output_grouped_by_date_ip_and_violated_rule(
        args.input,
        valid_ip_address_matcher_func,
        logger,
        args.output
    )
    logger.info("total_rows_processed {}".format(total_rows_processed))

    seconds_spent = round(time.time() - tm_begin, 2)
    stat_data = [
        ["Parameter", "Value"],
        ["Records processed", total_rows_processed],
        ["Added records", added_rows_count],
        ["Time spent", seconds_spent]
    ]
    table = terminaltables.AsciiTable(stat_data)
    logger.info("\n{}".format(table.table))
    logger.info("application finished in {}s".format(seconds_spent))
    return 0


if __name__ == "__main__":
    res = main()
    exit(res)

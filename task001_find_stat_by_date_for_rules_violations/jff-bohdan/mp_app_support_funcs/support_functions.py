import codecs
import csv
import logging
from collections import namedtuple
from typing import Dict, List

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


def map_csv_header_to_row_indexes(item_row, required_columns) -> Dict[str, int]:
    res = dict()

    for column_index, column_name in enumerate(item_row):
        if column_name not in required_columns:
            continue

        res[column_name] = column_index

    return res


def read_csv_header(input_file_name, delimiter, quote_char) -> List[str]:
    with codecs.open(input_file_name, "r", "utf-8") as input_csvfile:
        data_reader = csv.reader(input_csvfile, delimiter=delimiter, quotechar=quote_char)
        header = next(data_reader)

        return header

import codecs
import csv
import ipaddress
from typing import List

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


def generate_forbidden_networks(networks=None) -> List[ipaddress.ip_network]:
    ret = list()
    networks = networks if networks else FORBIDDEN_NETWORKS

    for ip_address in networks:
        ret.append(ipaddress.ip_network(ip_address))

    return ret


def parse_header_indexes(item_row, ip_columns):
    res = []

    ip_columns = list(set(ip_columns))
    for column_index, column_name in enumerate(item_row):
        if column_name not in ip_columns:
            continue

        res.append(column_index)

    return res


def read_csv_header(input_file_name, delimiter, quote_char) -> List[str]:
    with codecs.open(input_file_name, "r", "utf-8") as input_csvfile:
        data_reader = csv.reader(input_csvfile, delimiter=delimiter, quotechar=quote_char)
        header = next(data_reader)

        return header


def produce_mapping_report(logger, forbidden_items_cache):
    table_data = [
        ["IP Address", "Masked IP Address"]
    ]
    for key, value in forbidden_items_cache.items():
        table_data.append([key, value])
    table = terminaltables.AsciiTable(table_data)
    logger.info("Masked IP Addresses:\n{}".format(table.table))

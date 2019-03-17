import codecs
import csv
import logging


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


def iterate_over_csv_input_data(input_file_name, delimiter, quote_char):
    with codecs.open(input_file_name, "r", "utf-8") as input_csvfile:
        data_reader = csv.reader(input_csvfile, delimiter=delimiter, quotechar=quote_char)
        for row_data in data_reader:
            yield row_data

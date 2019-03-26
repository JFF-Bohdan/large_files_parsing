import csv
import logging
import multiprocessing as mp
import os
import sys
import time

import terminaltables

abs_file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(abs_file_dir)
sys.path.append(os.path.join(abs_file_dir, "."))
sys.path.append(os.path.join(abs_file_dir, "../.."))

from mp_app_processors.csv_reader import csv_reader_process  # noqa
from mp_app_processors.data_processor import process_data  # noqa
from mp_app_support_funcs.support_functions import init_logger, map_csv_header_to_row_indexes, read_csv_header  # noqa
from mp_app_support_funcs.command_line_args_initializer import parse_command_line  # noqa

PROCESSING_TASK_SIZE = 10000


def write_results_to_file(output_file_name, result_data):
    keys = result_data.keys()
    keys = sorted(keys, key=(lambda x: tuple([x.date, x.ip, x.violation_rule])))

    dialect = csv.excel
    dialect.delimiter = ";"
    header = ["date", "ip", "violated rule", "violations count"]

    added_rows_count = 0
    with open(output_file_name, "w", newline="") as output_csv_file:
        writer = csv.writer(output_csv_file, dialect=dialect)
        writer.writerow(header)
        for key in keys:
            violations_count = result_data[key]
            writer.writerow([key.date, key.ip, key.violation_rule, violations_count])
            added_rows_count += 1

    return added_rows_count


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

    tm_begin = time.time()

    logger.info("going to process file '{}'".format(args.input))
    logger.info("going to generate results into '{}'".format(args.output))
    logger.info("Number of CPU: {}".format(mp.cpu_count()))

    csv_header = read_csv_header(args.input, args.delimiter, args.quote_char)
    logger.info("csv_header: '{}'".format(csv_header))

    required_columns_indexes = map_csv_header_to_row_indexes(csv_header, ["dst", "timestamp", "message"])
    logger.info("required_columns_indexes: {}".format(required_columns_indexes))

    tasks_to_accomplish = mp.Queue()

    manager = mp.Manager()
    global_results_dict_lock = mp.Lock()
    global_results_dict = manager.dict()

    total_records_processed = mp.Value("i", 0)
    total_records_read = mp.Value("i", 0)

    tasks_generator_process = mp.Process(
        target=csv_reader_process,
        args=(
            args.input,
            args.delimiter,
            args.quote_char,
            args.output_limit,
            tasks_to_accomplish,
            total_records_read,
            args.periodic_interval,
            PROCESSING_TASK_SIZE
        )
    )

    logger.info("starting process for tasks reading")
    tasks_generator_process.start()

    required_task_processing_processors_count = max((mp.cpu_count() // 2), 1)
    logger.info("required_task_processing_processors_count: {}".format(required_task_processing_processors_count))

    list_of_processing_processes = list()
    for _ in range(required_task_processing_processors_count):
        data_processor = mp.Process(
            target=process_data,
            args=(
                tasks_to_accomplish,
                args.ip_address_mask,
                required_columns_indexes,
                total_records_processed,
                args.periodic_interval,
                global_results_dict,
                global_results_dict_lock
                # partial_results_queue
            )
        )

        list_of_processing_processes.append(data_processor)
        data_processor.start()

    logger.info("joining reader")
    tasks_generator_process.join()
    logger.info("len(tasks_to_accomplish): {}".format(tasks_to_accomplish.qsize()))

    logger.info("adding poison pill for filter processes")
    for _ in range(required_task_processing_processors_count):
        tasks_to_accomplish.put(None)

    logger.info("joining processors")
    for p in list_of_processing_processes:
        p.join()

    logger.info("writing results to file")

    seconds_spent = round(time.time() - tm_begin, 3)
    records_per_second = round((total_records_processed.value / seconds_spent), 3)
    result_rows_count = write_results_to_file(args.output, global_results_dict)

    seconds_spent = round(time.time() - tm_begin, 3)

    logger.info("len(global_results_dict): {}".format(len(global_results_dict)))
    stat_data = [
        ["Parameter", "Value"],
        ["Total records read", total_records_read.value],
        ["Records processed", total_records_processed.value],
        ["Result records count", result_rows_count],
        ["Records per second", records_per_second],
        ["Time spent", seconds_spent]
    ]
    table = terminaltables.AsciiTable(stat_data)
    logger.info("\n{}".format(table.table))
    logger.info("application finished in {}s".format(seconds_spent))
    return 0


if __name__ == "__main__":
    res = main()
    exit(res)

import logging
import multiprocessing as mp
import os
import sys
import time

abs_file_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(abs_file_dir)
sys.path.append(os.path.join(abs_file_dir, ".."))

from data_cleaner.support_functions.ip_cleaner_command_line_args_initializer import parse_command_line  # noqa
from data_cleaner.support_functions.support_functions import init_logger  # noqa
from data_cleaner.ip_cleaner_processors.csv_writer import csv_writer_process  # noqa
from data_cleaner.ip_cleaner_processors.csv_reader import csv_reader_process  # noqa
from data_cleaner.ip_cleaner_processors.ip_filter import csv_filter_process  # noqa
from data_cleaner.support_functions.ip_cleaner_functions import parse_header_indexes, read_csv_header  # noqa


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

    logger.info("going to process file '{}'".format(args.input))
    logger.info("going to generate results into '{}'".format(args.output))
    logger.info("Number of CPU: {}".format(mp.cpu_count()))

    tm_begin = time.time()

    csv_header = read_csv_header(args.input, args.delimiter, args.quote_char)
    logger.info("csv_header: '{}'".format(csv_header))

    indexes_to_filter = parse_header_indexes(csv_header, ip_columns)
    logger.info("indexes_to_filter: {}".format(indexes_to_filter))

    tasks_to_accomplish = mp.Queue()
    write_tasks = mp.Queue()

    tasks_generator_process = mp.Process(
        target=csv_reader_process,
        args=(args.input, args.delimiter, args.quote_char, args.output_limit, tasks_to_accomplish,)
    )

    results_writer_process = mp.Process(
        target=csv_writer_process,
        args=(args.output, args.delimiter, args.quote_char, csv_header, write_tasks)
    )

    logger.info("starting process for tasks reading")
    tasks_generator_process.start()

    logger.info("starting process for tasks results writing")
    results_writer_process.start()

    required_task_processing_processors_count = mp.cpu_count()
    list_of_processing_processes = list()
    for _ in range(required_task_processing_processors_count):
        data_processor = mp.Process(
            target=csv_filter_process,
            args=(tasks_to_accomplish, write_tasks)
        )

        list_of_processing_processes.append(data_processor)
        data_processor.start()

    logger.info("joining reader")
    tasks_generator_process.join()
    logger.info("len(tasks_to_accomplish): {}".format(tasks_to_accomplish.qsize()))

    logger.info("adding poison pill")
    for _ in range(required_task_processing_processors_count):
        tasks_to_accomplish.put(None)

    logger.info("joining processors")
    for p in list_of_processing_processes:
        p.join()

    write_tasks.put(None)
    logger.info("joining writer")
    results_writer_process.join()

    logger.info("task processing complete")
    time_elapsed = round(time.time() - tm_begin, 3)

    logger.info("application finished in {}".format(time_elapsed))
    return 0


if __name__ == "__main__":
    res = main()
    exit(res)

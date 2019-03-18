import codecs
import csv
import logging
from multiprocessing import current_process

from data_cleaner.support_functions.support_functions import init_logger


def csv_reader_process(
        input_file_name,
        delimiter,
        quote_char,
        output_limit,
        output_tasks_queue,
        global_total_records_processed,
        periodical_print_interval,
        task_chunk_size
):
    current_process_name = current_process().name

    logger = init_logger(current_process_name, logging.DEBUG)
    logger.info("reader process started")

    total_records_processed = 0
    task = list()
    with codecs.open(input_file_name, "r", "utf-8") as input_csvfile:
        data_reader = csv.reader(input_csvfile, delimiter=delimiter, quotechar=quote_char)

        for row_index, row_data in enumerate(data_reader):
            if not row_index:
                continue

            task.append((row_index, row_data, ))
            total_records_processed += 1

            if len(task) >= task_chunk_size:
                output_tasks_queue.put(task)
                task = list()

            if periodical_print_interval and (row_index % periodical_print_interval == 0):
                logger.info("reader process [{}] records generated: {}".format(current_process_name, row_index))

            if output_limit and (row_index >= output_limit):
                break

    if task:
        output_tasks_queue.put(task)

    global_total_records_processed.value += total_records_processed
    return True

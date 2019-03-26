import codecs
import csv
import logging
import queue
from multiprocessing import current_process

from data_cleaner.support_functions.support_functions import init_logger


def csv_writer_process(output_file_name, delimiter, quote_charm, header_row, queue_to_write, periodic_print_interval):
    writen_rows_count = 0

    current_process_name = current_process().name

    logger = init_logger(current_process_name, logging.DEBUG)
    logger.info("writer process started")

    with codecs.open(output_file_name, "w", "utf-8") as output_file:
        data_writer = csv.writer(output_file, delimiter=delimiter, quotechar=quote_charm)
        data_writer.writerow(header_row)

        while True:
            try:
                write_tasks = queue_to_write.get(block=True)
                if write_tasks is None:
                    break

                for row_to_write in write_tasks:
                    data_writer.writerow(row_to_write)

                    writen_rows_count += 1

                    if periodic_print_interval and (writen_rows_count % periodic_print_interval == 0):
                        logger.info(
                            "writer[{}] processed records count: {}".format(
                                current_process_name,
                                writen_rows_count
                            )
                        )

            except queue.Empty:
                break

    logger.info("writer process finished")
    return True

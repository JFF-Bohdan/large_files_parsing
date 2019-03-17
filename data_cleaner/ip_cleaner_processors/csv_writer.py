import codecs
import csv
import queue
from multiprocessing import current_process

from .shared_settings import PERIODICAL_PRINT_INTERVAL


def csv_writer_process(output_file_name, delimiter, quote_charm, header_row, queue_to_write):
    current_process_name = current_process().name
    writen_rows_count = 0

    with codecs.open(output_file_name, "w", "utf-8") as output_file:
        data_writer = csv.writer(output_file, delimiter=delimiter, quotechar=quote_charm)
        data_writer.writerow(header_row)

        while True:
            try:
                write_tasks = queue_to_write.get(block=True)
                if write_tasks is None:
                    return True

                for row_to_write in write_tasks:
                    data_writer.writerow(row_to_write)

                    writen_rows_count += 1

                    if writen_rows_count % PERIODICAL_PRINT_INTERVAL == 0:
                        print("writer[{}] processed records count: {}".format(current_process_name, writen_rows_count))

            except queue.Empty:
                break

    return True

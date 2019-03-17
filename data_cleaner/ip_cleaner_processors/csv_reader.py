import codecs
import csv
from multiprocessing import current_process


def csv_reader_process(input_file_name, delimiter, quote_char, output_limit, output_tasks_queue):
    current_process_name = current_process().name

    with codecs.open(input_file_name, "r", "utf-8") as input_csvfile:
        data_reader = csv.reader(input_csvfile, delimiter=delimiter, quotechar=quote_char)
        for row_index, row_data in enumerate(data_reader):
            if not row_index:
                continue

            if row_index % 10000 == 0:
                print("reader[{}] records generated: {}".format(current_process_name, row_index))

            output_tasks_queue.put(row_data)
            if output_limit and (row_index >= output_limit):
                break

    return True

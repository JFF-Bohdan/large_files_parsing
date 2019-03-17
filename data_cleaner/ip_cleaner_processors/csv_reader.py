import codecs
import csv
from multiprocessing import current_process

from .shared_settings import FILTERING_TASK_SIZE, PERIODICAL_PRINT_INTERVAL


def csv_reader_process(input_file_name, delimiter, quote_char, output_limit, output_tasks_queue):
    current_process_name = current_process().name

    task = list()
    with codecs.open(input_file_name, "r", "utf-8") as input_csvfile:
        data_reader = csv.reader(input_csvfile, delimiter=delimiter, quotechar=quote_char)

        for row_index, row_data in enumerate(data_reader):
            if not row_index:
                continue

            task.append(row_data)
            if len(task) >= FILTERING_TASK_SIZE:
                output_tasks_queue.put(task)
                task = list()

            if row_index % PERIODICAL_PRINT_INTERVAL == 0:
                print("reader[{}] records generated: {}".format(current_process_name, row_index))

            if output_limit and (row_index >= output_limit):
                break

    if task:
        output_tasks_queue.put(task)

    return True

import queue
from multiprocessing import current_process

# def fake_ip_generator_func(_):
#     res = [random.randint(1, 254), random.randint(0, 254), random.randint(1, 254), random.randint(1, 254)]
#     return ".".join([str(item) for item in res])


def csv_filter_process(input_tasks_queue, output_tasks_queue):
    processed_rows = 0
    current_process_name = current_process().name

    while True:
        try:
            row_to_process = input_tasks_queue.get(block=True)
            if row_to_process is None:
                return True

            output_tasks_queue.put(row_to_process)

            processed_rows += 1

            if processed_rows % 10000 == 0:
                print("filter[{}] processed records count: {}".format(current_process_name, processed_rows))

        except queue.Empty:
            break

    return True

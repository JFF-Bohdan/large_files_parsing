import csv
import os
from collections import defaultdict
import argparse
import time
import datetime

IP_FILTER = '192.168'


def cleaned_data_generator(data_file_name):
    with open(data_file_name) as data_file:
        csv_reader = csv.reader(data_file)
        try:
            for row in csv_reader:
                if row[1].startswith(IP_FILTER):
                    yield row[0], row[1], row[2]
        except IndexError:
            print("Not properly row format:\n '{}' ".format(row))


def result_lines_generator(data_file_name):
    dates_dict = defaultdict(lambda: defaultdict(int))
    for date_time_str, ip, mess in cleaned_data_generator(data_file_name):
        date_str = date_time_str[:10]
        dates_dict[date_str][(ip, mess)] += 1

    for date, ip_mess_dict in dates_dict.items():
        for (ip, mess), count in ip_mess_dict.items():
            yield ' '.join([date, ip, mess, str(count), '\n'])


def create_stat_file(input_file_name, output_file_name):
    with open(output_file_name, "w") as f:
        f.writelines(result_lines_generator(input_file_name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', type=str)
    parser.add_argument('output_file', type=str)
    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print("Error! File does not exist!:\n{} ".format(args.input_file))
        exit(1)

    start_time = time.time()

    create_stat_file(args.input_file, args.output_file)

    time_delta = time.time() - start_time
    running_time = str(datetime.timedelta(seconds=time_delta))
    print("Time of running: {}".format(running_time))

# Task 001 find statistics about violated rules grouped by date and IP address

## Data structure description

* `timestamp` - timestamp of violation accident;
* `dst` - IP address that violated rule;
* `message` - violated rule name.

Some other columns may be added to the input data file. They should be ignored.

## Task description

Parse CSV file with given structure and provide output with structure:

* `date` - date (without time) of violation;
* `ip` - IP address which violated rule;
* `violated rule` - name of violated rule;
* `violations count` - violations count for given IP address on given date.

Exclude all IP addresses which not starts from `192.168.`.

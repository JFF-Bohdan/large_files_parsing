# IP Cleaner

This tool may be used to clean input datasets from IP addresses that
should be excluded.

Usage:

```
python data_cleaner\ip_cleaner.py --input d:\csv_playground\log.csv --output d:\csv_playground\limited_clean_log.csv --ip_columns "dst,ip" --delimiter "," --quote_char "\"" --periodic_interval 50000 --output_limit 250000
```

Where:

* `--input .\task001_find_stat_by_date_for_rules_violations\data\data.csv` - path to input file
* `--output .\task001_find_stat_by_date_for_rules_violations\data\clean_data.csv` - path to output file
* `--ip_columns "dst,ip"` - list of column names that should be processed
* `--delimiter ","` - delimiter char for CSV processing
* `--quote_char "\""` - quote char for CSV processing


In case if required you also may add keys:

* `--produce_report` - in this key after processing report will be generated.
In this report you may find original IP address and masked IP address;
* `--periodic_interval 10000` - periodical interval between printing to console.
* `--output_limit 250000` - limit output to 250000 rows

Enjoy!



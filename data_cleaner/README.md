# IP Cleaner

This tool may be used to clean input datasets from IP addresses that
should be excluded.

Usage:

```
python .\data_cleaner\ip_cleaner.py --input .\task001_find_stat_by_date_for_rules_violations\data\data.csv --output .\task001_find_stat_by_date_for_rules_violations\data\clean_data.csv --ip_columns "dst,ip" --delimiter "," --quote_char "\""
```

Where:

* `--input .\task001_find_stat_by_date_for_rules_violations\data\data.csv` - path to input file
* `--output .\task001_find_stat_by_date_for_rules_violations\data\clean_data.csv` - path to output file
* `--ip_columns "dst,ip"` - list of column names that should be processed
* `--delimiter ","` - delimiter char for CSV processing
* `--quote_char "\""` - quote char for CSV processing

Enjoy!
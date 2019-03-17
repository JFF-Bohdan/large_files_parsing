# Data Cleaning tasks

Special tools required to clean input datasets. List of required tools
mentioned below. Fastest implementation should be used as actual tool
for cleaning.

## IP addresses removal tool

### General information

This tool should be used to remove IP addresses that belongs to specified
networks from input datasets.

### Requirements

* any columns list should be supported in dataset that should be cleaned;
* tool should support different delimiters and quote symbols;
* list of specified networks may be hardcoded or stored in separate file;
* be able to remove IP addresses from any column in given dataset. List of
columns should be predefined (common list of column names) with ability to
define custom list in command line arguments;
* columns that should't be cleaned should be untouched;
* IP addresses that should be cleaned should be replaced with newly
generated and replacement should be consistent through all dataset.

### Expected command line arguments

* `--input` - input file name;
* `--output` - output file name;
* `--ip_columns` - list of columns that should be cleaned, example: `"dst,ip"`
* `--delimiter` - delimiter for CSV files parsing. Example: `","`
* `--quote_char` - quote char for CSV files parsing. Example: `"\""`

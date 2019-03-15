# Large Files Parsing

## General information and rules

Here you can find a set of tasks for logs parsing, scripts for datasets
generation and solutions from different developers.

Please do not upload any datasets with sensitive information or
big `.csv` files. Only small files (less than few hundreds kilobytes)
datasets should be uploaded after security clearance.

Tasks should be solved using Python 3.

Execution of solution implementation should not generate any files that
will be stored in repository.

## Folder structure

Each sub-folder contains task or group of tasks. Inside each folder
file `task_description.md` may be found. This file contains
information about task. Also folder `data` may be found in each
task related folder. This folder should contain example datasets.

Inside each task folder task with name of developer may be created.
Inside this folder any strcutre may appear.

In case if acceptable `app.py` should be used as name for solution.

Additional `README.md` may be provided per implementation if required.
This document may include information:

* how to properly install packages (if required);
* how to start application with examples of command line.

## Command line arguments

Each script should take path to the input files and path to the output
files as command line arguments.

Expected argument names:

* `--input` - path to the input file name or folder with predefined input files;
* `--output` - path to the output file or folder where output files (with
predefined names in case if multiple files should be generated) should
be saved

## `requirements.txt`

Each implementation may require additional packages to be installed. In
this case all these packages should be listed in `requirements.txt`

Enjoy!
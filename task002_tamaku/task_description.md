# tamaku
Tamaku game solver

## General information

I got this task on interview for one company. And I was amazed by task. So, here is my solution of this task.

As a part of task candidate must provide source code and output file for large input file. Large input file provided by company, you can 	find it in `data/large_data.zip`

Name of game was changed according to company requirements.

This is new version, which contains original file with data (48M in `.zip`) and uses `multiprocessing` for CPU utilization and `mmap` for super-fast file loading.

## Game rules

Pat and Mat are playing a game called Tamaku on their pocket calculator. The game begins
with a positive integer displayed on the calculator screen. Pat starts the game and then they
alternate their turns. At each turn:

1. A player computes how many 1s are in the binary representation of the integer d on the
display. E.g. for 17 it is two 1s because 17=b10001
2. The player picks up a positive integer k<d with exactly one 1 in its binary representation,
e.g. 8=b1000, and subtracts it from a number displayed on the calculator.
3. The player computes 1s in the binary representation of the new number. E.g.
17-8=9=b1001, which contains two 1s.
4. If the number of 1s in the former number and the new number differs, the player loose the
game. If the player can’t make any move, he looses the game.

Both players are pretty clever and play the game optimally. Still, there can be only one
winner. Can you determine who wins, if you know the starting number?

## Task

**Input**

The first line of the input contains G, a number of games played. 0 < G < 10,000,000Then
follows G lines, each containing a starting integer 0 < N < 1,000,000,000.

**Output**

For each game print Pat if the first player has winning strategy, otherwise print Mat.

**Sample Input**

```
7
1
2
3
10
17
47
999999999
```

**Sample Output**

```
MAT
PAT
MAT
PAT
PAT
PAT
MAT
```

**Explanation**

In the first game, N=1 and Pat has no valid move, and looses.
In the second game, N=2. Pat subtracts 1, and Mat looses the game.

## Usage

### Before execution

You **can** use `make` to automate boring boring stuff, especially typing long command lines. So, you can use rules coded in my `Makefile`. This file configured to use in Windows, so in Linux you still need use long command lines.

Make for Windows can be found [here](http://gnuwin32.sourceforge.net/packages/make.htm).

Also, you need install `virtualenv` for secure and safe development. To do this you just need execute:

`pip install virtualenv`

To setup environment you just need exec `make` in project root directory. It will setup virtual environment and will install all needful packages.

But if you just need run application to solve tasks, you can execute it just by using system `Python`.

### Solving input taks
If you just process input file use:

`python tamaku.py -i ./data/small_data.data`


Where `./data/small_data.data` path to your input task file. In this case program will read input file and then produce solutions to console.

Or you can type `make run_small` to execute solver against small data file.

### Solving input tasks and measuring execution time
If you want program to be a little bit more verbose, for example calculate execution time, you can use `-w` switch:

`python tamaku.py -w -i ./data/small_data.data`

Or you can use `-v` (verbose) switch to measure data load time and execution time.

In this case, after all output lines, program will print information about execution time.

### Limiting output

In case when you developing new features and profiling program, you can set limit for output. In this case, program will be terminated when **<count>** solutions will be found. For example, if your file contains 10M tasks, you can solve just 10K tasks and measure execution time to predict time needed for 10M tasks.

Use `-l <count>` command line argument, where `<count>` your limit.

`python tamaku.py -l 10000 -w -t ./tmp -i ./data/large_data.zip`

In given example output will be limited to 10000 lines.

Also, we need specify temporary directory where input file will be decompressed if necessary.

Note: program will unzip all files with `.zip` extenstion into specified temp directory before execution.

Or you can just type `make run_large` using `make`.


### Executing application against file with 10M tasks

In case when you testing algorithm modifications you can use `-n` switch with ```-v``` switch

`python tamaku.py -w -t ./tmp -i ./data/large_data.zip -o ./results/result.data`

Warn: you should create directory for ouput (`./results` in this case) and temporary directory (`./tmp` in this case) before running application.

In given example:

* `-w` - measure execution time and print it to `stderr`
* `-t ./tmp` - specifies directory for files decompression;
* `-i ./data/large_data.zip` - specifies input file;
* `-o ./results/result.data` - specifies output file path.

Execution will take up to 10-15 minutes, depending to you hardware. Sample output 

Sample output on my laptop (i7-3632QM):

`Done @ 6.0m 35.93s - 10000000 tasks processed`

## Developing

Main points:

* to run test just execute `make tests`, it will check implementation against pre-defined data set;
* core function can be found in `./solver/solver_impl.py`;
* data processing (loading and orchestration) can be found in `./data_processor/data_processor.py` - it loads data using `mmap` and then executes data processing using multiprocessing.

Hope you like it. Enjoy!
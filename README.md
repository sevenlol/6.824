6.824: Distributed Systems
====================

This repository contains my code for [6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/index.html) labs.

Disclaimer: I did not take the class and am fairly new to Golang. Therefore, there are very likely mistakes that I did not notice. I tried to follow every instructions in the course website and pass all the tests.

This repository is following the 2018 spring version of the course.

### Directories

1. `src/main`: Client & integration tests for labs
2. `src/mapreduce`: Lab1 MapReduce
3. `src/raft`: Lab2 Raft

Other directories contain libraries downloaded from the course website.

### Lab1 MapReduce

[Lab1 website](https://pdos.csail.mit.edu/6.824/labs/lab-1.html)

Implement a MapReduce system, a word count mapper/reducer and an inverted index mapper/reducer.

Did not encounter much problems in this lab. The tasks are idempotent, so simply retry on failure will pass the tests. Good lab for practicing gorutines and other concurrency stuff in Go.

#### Run tests

In `src/main`, run `bash test-mr.sh`. Test result is shown in the image below.

![MapReduceResult](https://raw.githubusercontent.com/sevenlol/6.824/master/results/mapreduce.png)

### Lab2 Raft

<!-- https://raw.githubusercontent.com/sevenlol/6.824/master/results/raft_v1.png -->

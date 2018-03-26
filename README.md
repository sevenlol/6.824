6.824: Distributed Systems
====================

This repository contains my code for [6.824: Distributed Systems](https://pdos.csail.mit.edu/6.824/index.html) labs.

Disclaimer: I did **NOT** take the class and am fairly new to Golang. Therefore, there are very likely mistakes that I did not notice. I tried to follow every instructions in the course website and pass all the tests.

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

[Lab2 website](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)

Implement `Raft`, a concensus protocol. I hard stuck in 2-C for 1.5 days, not passing some of the unreliable tests. My mistake was to not read [students' guide](https://thesquareplanet.com/blog/students-guide-to-raft/) first and only read the paper. It turns out that there are a lot of details you must pay attention to. For example, you must not reset the election timer when receiving request from a node that is not the current leader (or not granting its vote request).

This does not matter in normal tests as it will only cause the nodes to take more time to reach concensus. However, in unreliable tests (tests with unreliable in its name), you need to elect a leader in a given time (10 seconds as default) after network partition healed. Otherwise the tests will fail. If you only fail unreliable tests, try to modify the following line (line 408 in this repo) in `config.go`'s `one` function: `for time.Since(t0).Seconds() < 10 {`. If by increasing the interval required to reach concensus (for me 13 is enough), you are able to pass the tests, then check everywhere mentioned in the guide that involve timer.

Another problem related to this (I encountered) is that when elected as leader, I did not send heartbeat immediately because I use a separate go routine to send out heartbeat periodically. Since you are only allowed to have heartbeat timer > 100ms, waiting potentially 100ms can cause another node (already voted for you) to timeout and become a candidate.

#### Run tests

In `src/raft`, run `go test`. All tests should pass within 4 minutes. The test results is shown below.

![RaftResult](https://raw.githubusercontent.com/sevenlol/6.824/master/results/raft_v1.png)

### Lab3 Fault-tolerant Key/Value Service

**TODO**
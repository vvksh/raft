# Raft consensus protocol

**Status: WIP**
-------------

This repo contains Go implementation of Raft consensus protocol. I am using instructions from lab 2 of [MIT's distributed systems course 6.824](https://pdos.csail.mit.edu/6.824/index.html) (from Spring 2020).

Here's summary of what's implemented in this project (from the lab instruction):

*In this lab you'll implement Raft as a Go object type with associated methods, meant to be used as a module in a larger service. A set of Raft instances talk to each other with RPC to maintain replicated logs. Your Raft interface will support an indefinite sequence of numbered commands, also called log entries. The entries are numbered with index numbers. The log entry with a given index will eventually be committed. At that point, your Raft should send the log entry to the larger service for it to execute.*

*You should follow the design in the [extended Raft paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf), with particular attention to Figure 2. You'll implement most of what's in the paper, including saving persistent state and reading it after a node fails and then restarts. You will not implement cluster membership changes (Section 6). You'll implement log compaction / snapshotting (Section 7) in a later lab.*

This project is divided into three parts

- **1**: Implement Raft leader election and heartbeats (AppendEntries RPCs with no log entries). The goal for Part 2A is for a single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost. See issues: [#1](https://github.com/vksah32/raft/issues/1), [#2](https://github.com/vksah32/raft/issues/2)

- **2** Implement the leader and follower code to append new log entries

- **3** Implement state persistence in raft so that it can survive reboot and resume service where it left off.



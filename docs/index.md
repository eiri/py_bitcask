# Bitcask

A Log-Structured Hash Table for Fast Key/Value Data.

## Motivation

A short undertaking to refresh my knowledge of Python programming and serve as an educational endeavor.

## Bitcask Overview

Bitcask is a high-performance key-value store designed for efficiently storing and retrieving data on disk. It utilizes a log-structured storage mechanism that organizes data into an append-only log, providing fast write and read operations. In Bitcask, when a write operation occurs, Bitcask appends the new data to a write-ahead log and updates an in-memory hash table for quick access. Reads are performed by first checking the hash table for the location of the data file and then retrieving the data directly from disk, ensuring constant-time access regardless of database size.

Bitcask employs a merge process to periodically compact data files and reclaim disk space. During a merge, Bitcask iterates over immutable data files and produces a set of compacted files containing only the latest versions of each key. This process helps prevent disk fragmentation and optimizes storage utilization over time. Bitcask's simplicity, coupled with its efficient read and write operations, makes it well-suited for use cases requiring fast and reliable key-value storage.

## Whitepaper

[Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)

## API

- open: directory
- get: key
- put: key, value
- list_keys
- fold: fun, acc
- merge
- sync
- close

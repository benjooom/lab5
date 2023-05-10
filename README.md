# Key-Value Store

Key-value stores are a fundamental building block in modern distributed systems, providing a simple and efficient way to store and retrieve data. In recent years, the popularity of key-value stores has increased significantly due to their ability to scale horizontally, tolerate failures, and support high-performance read and write operations.

The repository showcases our implementation of a Redis-like key-value store.

## Implementation Details

### Data Types

This key-value store has implemented various data types.

1. (String): The original key-value store implementation was a key to value.
2. (List): Keeps a list of string values in chronological order. Implemented using a slice.
3. (Set): Keeps unique values in a set. Implemented using a map.
4. (SortedSet): Maintains unique values sorted based on supplied rank.

### New APIs

- MultiSet:
  - Gaurantees: is atomic in xyz context

- Compare-And-Swap:
  - Gaurantees: will only update the key's value if the present key matches the expected key.

### Deployment

INSERT: how to change the IPs to run on your computers!?

Run command to 

## Testing

cd into kv/test/ and run go execute all related test (client-side, server-side, integration)
1. go test -run=TestList
2. go test -run=TestSet
3. go test -run=TestSortedSet
4. go test -run=TestMultiSet
5. go test -run=TestCAS

## Demo

INSERT: Google Drive Links
1. [Deployment on Multiple Nodes](LINK)
2. [Shard Migrations Proof](LINK)

## Group Work

### Alice (ava26):
- Wrote MultiSet client and server code, proto modifications
- Wrote MultiSet test cases (unit and integration), wrote corresponding write-up
- Modified stress tester to run queries using list and sorted set
- Compared latency for various cases with stress tester

### Ben ():

### Gabe (gnd6):
- Learning how to use prototoc to generate the proto files
- Restructuring backend data store to be compatible with multiple data types
- Implementing the APIs (Create, Append, Check, Remove, etc) for the new back-end datatypes List, Set, and SortedSet
- Adjusting the testing script to be able to connect to the new APIs (test_setup.go & test_clientpool.go)
- Writing tests for the new data type APIs (Create, Append, Check, Remove, Pop, GetRange)
- Writing portion of the write-up related to the datatypes and researching related works.
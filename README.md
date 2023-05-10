# Key-Value Store

Description

## Implementation Details

### Data Types

This key-value store has implemented various data types.

1. (String): The original key-value store implementation was a key to value
2. (List): xyz
3. (Set): xyz
4. (SortedSet): xyz

### New APIs

- MultiSet:
  - Gaurantees: is atomic in xyz context

- Compare-And-Swap:
  - Gaurantees: will only update the key's value if the present key matches the expected key.

### Deployment

cd into kv/test/ and run go test
1. go test -run=TestList
2. go test -run=TestSet
3. go test -run=TestSortedSet
4. go test -run=TestMultiSet
5. go test -run=TestCAS

INSERT: how to change the IPs to run on your computers!?

## Testing

INSERT: how to run the testing script

## Demo

INSERT: Google Drive Links
1. [Deployment on Multiple Nodes](LINK)
2. [Shard Migrations Proof](LINK)

## Group Work

### Alice ():

### Ben ():

### Gabe (gnd6):
- Learning how to use prototoc to generate the proto files
- Restructuring backend data store to be compatible with multiple data types
- Implementing the APIs (Create, Append, Check, Remove, etc) for the new back-end datatypes List, Set, and SortedSet
- Adjusting the testing script to be able to connect to the new APIs (test_setup.go & test_clientpool.go)
- Writing tests for the new data type APIs (Create, Append, Check, Remove, Pop, GetRange)
- Writing portion of the write-up related to the datatypes and researching related works.
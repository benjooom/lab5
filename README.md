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

## Deployment

Deploying our system on active and distributed nodes in a network is pretty easy. You simply edit the shard maps in the shardmaps folder to assign each node the correct IP. A computer's/server's IP address can be obtained by running `ifconfig` in shell. Once you write your shard map to define the system specifications and node/IP pairs, run the system on each computer/server as the node which holds its IP in the shard map. For example, if my computer's IP address is recorded as belonging to node 1 in the shard map, I would run: `go run cmd/server/server.go --shardmap shardmaps/[shardmap_name.json] --node n1`.

## Testing

cd into kv/test/ and run go execute all related test (client-side, server-side, integration)
1. go test -run=TestList
2. go test -run=TestSet
3. go test -run=TestSortedSet
4. go test -run=TestMultiSet
5. go test -run=TestCAS

## Demo

1. In the zip you will find a video demonstrating a deployment of our implementation of a sharded key/value store. The test case is defined by the `test-2-node.json` shard map. To deploy a system on active nodes, you simply input the correct IP address for each node in the map. You can obtain a computer's IP by running `ifconfig`. There are two nodes which both share a single shard. On the right computer node 1 runs, while on the left one node 2 runs. The stress tester is then activated. After some successful requests, we shut down node 1 and observe that the stress tester is unable to reach it. Then we reactivate node 1, which does a shard migration (as we can see in the log) and starts usccesfully serving requests again. We know both shards have reached a state of consesus because there are no errors present in the stress tester after a few seconds.

2. In the zip you will find a video demonstrating active shard migration of our new data types. In the demo, there are two running nodes which both host a single shard. We show that you are able to get and set strings, lists of strings, and sets of strings. Additionally, the demo shows active shard migration and copying of data (which now uses grpc streams and warmup) after nodes are shut down and then brought back up. The node that copies data to the shard logs that it is "Adding [key] to stripe," while the node sending the key/value pairs shows a list of the content it sends through its stream (e.g. shown in video: "Sending ShardContents(): key: lina, StringValue: , ListValue: [], SetValue: map[jack:true linda:true], SortedSetValue: {<nil> <nil> 0 0 map[]}, StringExpiry: 2023-05-10 15:00:05.195666 -0400 EDT m=+128.535055231, ListExpiry: 2023-05-10 15:00:05.195667 -0400 EDT m=+128.535055352, SetExpiry: 2023-05-10 17:46:27.23223 -0400 EDT m=+10110.576718739, SortedSetExpiry: 2023-05-10 15:00:05.195667 -0400 EDT m=+128.535055586").

## Group Work

### Alice (ava26):
- Wrote MultiSet client and server code, proto modifications
- Wrote MultiSet test cases (unit and integration), wrote corresponding write-up
- Modified stress tester, adding new flags and functions, to run queries using list and sorted set
- Compared latency for various cases with stress tester
- Wrote MultiSet portion of the write-up, added generated graphs, and helped write other sections (relevant work, abstract, conclusion, etc.) 

### Ben (bm746):
- Learned how to use protoc to manipulate and generate proto files.
- Implemented get and set commands to get or set entire lists and sets store in our key/value store.
- Wrote tests to check implementation of the above and tested with locally running nodes.
- Implemented shard migration with shard warmup and streaming. Tested implementation through newly created commands on locally running nodes.
- Deployed and tested system on two seperate computers (with Alice) showing a real-life application of our system.
- Writing up related portions in write-up after conducting relevant research.
### Gabe (gnd6):
- Learning how to use prototoc to generate the proto files
- Restructuring backend data store to be compatible with multiple data types
- Implementing the APIs (Create, Append, Check, Remove, etc) for the new back-end datatypes List, Set, and SortedSet
- Adjusting the testing script to be able to connect to the new APIs (test_setup.go & test_clientpool.go)
- Writing tests for the new data type APIs (Create, Append, Check, Remove, Pop, GetRange)
- Writing portion of the write-up related to the datatypes and researching related works.

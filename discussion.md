# Discussions

## A4

Q: What tradeoffs did you make with your TTL strategy? How fast does it clean up data and how expensive is it in terms of number of keys?

Q: If the server was write intensive, would you design it differently? How so?

The TTL strategy we decided to use involved goroutines running for each stripe in the background and periodically scanning the all stripes and all the entry TTLs to determine deletion. One tradeoff to this method is that the values won't be immediately removed from the cache when they're stale but rather must wait until the goroutine is done sleeping. The goroutine must go through all the entries in the cache and check their TTLs each time which increases overhead. However, this does come with the advantage of having a simpler implementation and being easier to debug issues with TTLs and evictions.

A write-intensive server would be especially problematic with our implementation, since there would be high lock contention. Our strategy uses a read lock to scan the stripe for stale values and then a write lock to delete them. In a write-heavy environment, the relatively long held read-lock could easily clash with the higher volume of write-locks, thus increasing system latency. Therefore, a strategy to handle the TTL deletion would be to increase the background goroutine sleeptime from 1 second to perhaps a higher value. This would allow for less lock contention during writes, with the tradeoffs of higher memory usage.

## B2

Q: What flaws could you see in the load balancing strategy you chose? Consider cases where nodes may naturally have load imbalance already -- a node may have more CPUs available, or a node may have more shards assigned.

Q: What other strategies could help in this scenario? You may assume you can make any changes to the protocol or cluster operation.

We chose the random load-balancing strategy. One flaw is that if there is a shard with only a few nodes, it might, by random chance, select one node more often and have an imbalanced node. Another (and more probable) flaw is that even if there is a relatively balanced distribution of nodes, it is only optimal if the nodes are well-suited to handle similar loads. As stated in the question, some nodes may have more CPUs or resources available, and should therefore handle more requests, whereas other nodes may be shared between multiple shards, and should therefore have fewer requests in an optimal situation. In these scenarios, possible load-balancing strategies may include using weighing to make some nodes likelier to be selected than others. Another scenario could include implementing health checks to monitor the utilization of each node and selecting nodes with lower resource utilization.


## B3

Q: For this lab you will try every node until one succeeds. What flaws do you see in this strategy? What could you do to help in these scenarios?

If certain nodes are disconnected, requests to shards may still try to connect to these nodes as a part of this strategy, which could lead to increases in latency. One possible solution to decrease latency if multiple nodes are tried concurrently, as GET is idempotent. However, this would also have a trade-off of increasing the load on the nodes. This would be especially unideal if the nodes are all healthy, as then, multiple nodes would be contacted when only a single node would suffice.

Another possible flaw with this strategy is that if there are a high number of requests, this could overload many of the replicas and overwhelm the resources. Instead of just overwhelming a single node, this could potentially inundate all of the servers, as all of the requests are sent to each server. A fix for this strategy would be to have a waiting period between requests, which does have a slight latency tradeoff. A better fix would be to send health checkups to monitor the utilization of each node. If the utilization is higher than a certain threshold, then the client could wait until the utilization is lower to send the request, preventing the client from overloading the nodes.

## B4
Partial failures could lead to inconsistent replicas (i.e. some of the nodes are missing the new key-value pair or don't properly update the pair). One possible anomaly is that the client may get different results from calling GET, an idempotent function, twice. For instance, it may be directed to one node, which properly updated the key-value pair, the first time, and then another node, which still has the old copy, the second time.

## D2
### Experiment 1: QPS
1 - DEFAULT (Set: 30 QPS, Get: 100 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json 
Get requests: 6020/6020 succeeded = 100.000000% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 6019/6019 = 100.000000%
Total requests: 7840 = 130.662635 QPS
    Average latency (get): 568.16   (set): 704.67


2 - SET QPS to 250 (Set: 250 QPS, Get: 100 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --set-qps 250
Get requests: 6020/6020 succeeded = 100.000000% success rate
Set requests: 15020/15020 succeeded = 100.000000% success rate
Correct responses: 6017/6017 = 100.000000%
Total requests: 21040 = 350.660783 QPS
    Average latency (get): 536.4    (set): 529


3 - GET QPS to 250 (Set: 30 QPS, Get: 250 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --get-qps 250
Get requests: 15020/15020 succeeded = 100.000000% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 15019/15019 = 100.000000%
Total requests: 16840 = 280.662278 QPS
    Average latency (get): 231.24   (set): 139.33


4 - SET QPS & GET QPS to 2000 (Set: 2000 QPS, Get: 2000 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --get-qps 2000 --set-qps 2000
Get requests: 119994/119994 succeeded = 100.000000% success rate
Set requests: 119986/119986 succeeded = 100.000000% success rate
Correct responses: 119938/119938 = 100.000000%
Total requests: 239980 = 3999.615245 QPS
    Average latency (get): 129.43   (set): 206.4


5 - SET QPS & GET QPS to 25000 (Set: 25000 QPS, Get: 25000 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --set-qps 20000 --get-qps 2000
Get requests: 817792/817792 succeeded = 100.000000% success rate
Set requests: 841184/841184 succeeded = 100.000000% success rate
Correct responses: 782517/782517 = 100.000000%
Total requests: 1658976 = 27648.308072 QPS
    Average latency (get): 1048.53  (set): 918.64


DISCUSSION: We were testing to see whether our system could handle high QPS, and whether there would be a difference between high set-QPS and high-get QPS. We were surprised by the first set of results, in which higher queries per second for both get and set (i.e. 2000 QPS for both vs. default options) would result in lower latency. It seemed like increase get-QPS especially decreased latency. Perhaps since get relies on read locks, having a high number of read queries to grab a read lock can decrease read lock contention. However, the fifth trial, with extremely high set-QPS and get-QPS, demonstrates that there is a limit to how many queries the servers can handle. The latency is much higher than the default settings with lower QPS, so we suspect that even higher QPS could lead to slower requests as a result of lock contention.

### EXPERIMENT 2: Hot Keys & Get QPS

1 - DEFAULT (Set: 30 QPS, Get: 100 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --num-keys=1   
Get requests: 6020/6020 succeeded = 100.000000% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 5394/5394 = 100.000000%
Total requests: 7840 = 130.664297 QPS
    Average latency (get): 593.77 (set): 539.57


2 - Get: 300 QPS (Set: 30 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --num-keys=1 --get-qps 300 
Get requests: 18020/18020 succeeded = 100.000000% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 16174/16174 = 100.000000%
Total requests: 19840 = 330.661300 QPS
    Average latency (get): 432.4 (set): 389


3 - Get: 3000 QPS (Set: 30 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --num-keys=1 --get-qps 3000
Get requests: 179738/179738 succeeded = 100.000000% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 177354/177354 = 100.000000%
Total requests: 181558 = 3025.936349 QPS
    Average latency (get): 231 (set): N/A

4 - Get: 30000000 QPS (Set: 30 QPS)
go run cmd/stress/tester.go --shardmap shardmaps/test-3-node.json --num-keys=1 --get-qps 30000000
Get requests: 1413550/1413550 succeeded = 100.000000% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 1296996/1296996 = 100.000000%
Total requests: 1415370 = 23589.111139 QPS
    Average latency (get): 873.90

DISCUSSION: We wanted to test how hot keys could be impacted by a high volume of get requests. Similar to the last experiment, we found that up to a certain threshold, more get queries leads to decreased latency. This was an interesting result, but again, we figured that this may simply be due to read locks allowing multiple reads to access a critical section at a time, and relatively higher get QPS means that reads have control of the lock more often. However, an extremely high QPS (like 30000000) then leads to a jump in latency, demonstrating perhaps resources are exhausted, and a higher number of queries will overwhelm the system


### BUG:
Additionally, a bug we found in the tester was that our client wasn't properly setting TTLs -- it was accidentally converting them into nanoseconds instead of the milliseconds expected by the spec. This led to a roughly 2% failure in the stress tester, and once we fixed this bug, we were up to 100% accuracy again.

## Group Work

### Gabe
We decided to split the assignment's work according to the parts. I began working on part A server implementation. I worked on this early on so that people working on the Shard Migration wouldn't be blocked from starting their work. Once I finished by portion of the split work, I floated around helping my other project members when they had questions regarding the server I had implemented. We also all worked on the final rigorous tests together. We spent a lot of time trying to debug as to why we were only getting 98% accuracy but after logging both on the server and client end I discovered that the TTLs were being set for over a month. This then led me to discover that the TTLs were being converted to nanoseconds on the client side but interpretted and milliseconds on the server side.

### Alice
I worked on part B client implementation. Since this part was a little less heavy than other parts, I also worked on the part B discussion questions and part D experiments. I also attended office hours with Ross to discuss some of the details and nuances behind the discussion questions. I also wrote some unit test cases for expiration time since we had a bug in the client implementation that prevented the expirations from being handled properly.

### Ben
I worked on part C of the server implementation. I had to wait for Gabe to finish part A to get started with this, but he was very quick so I could get started early. I adjusted Gabe's part A implementation (to agree with part C) and finsihed part C and was passing the remaining server tests. However, when we did stress testing together as a group we encountered a bug where GET was returning expired values. We all spent a lot of time debugging this together and met with Xiao to discuss potential sources of the issue. Ultimately it was a type conversion error. I also contributed to a few of the extra tests of our server, primarily those relevant to part C.
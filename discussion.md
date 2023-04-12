# Discussions

## A4

The TTL strategy we decided to use involved goroutines running for each stripe in the background and periodically scanning the all the TTLs to determine deletion. One tradeoff to this method is that the value's won't be immediately removed from the cache when they're stale but rather must wait until the goroutine is done sleeping. The goroutine must go through all the entries in the cache and check their TTLs each time which increases overhead. However, this does come with the advantage of having a simpler implementation and being easier to debug issues with TTLs and evictions.

If the server was write intensive then it would be problematic with the current implementation. A read-lock is held while the entire stripe is scanned for stale values which then upgrades to a write-lock when actually deleting them. In a read heavy system, the long read-lock scanning the entire stripe won't cause many latency issues from waiting on locks. However, in a write heavy environment, the relatively long held read-lock could easily clash with the higher volume of write-locks, thus increasing system latency.

## B2
What flaws could you see in the load balancing strategy you chose? Consider cases where nodes may naturally have load imbalance already -- a node may have more CPUs available, or a node may have more shards assigned.

What other strategies could help in this scenario? You may assume you can make any changes to the protocol or cluster operation.

We chose the random load-balancing strategy. One flaw is that if there is a shard with only a few nodes, it might, by random chance, select one node more often and have an imbalanced node. Another flaw is that even if there is a relatively balanced distribution of nodes, it is only optimal if the nodes are well-suited to handle similar loads. As stated in the question, some nodes may have more CPUs available, and should therefore handle more requests, whereas other nodes may be shared between multiple shards, and should therefore have fewer requests. In these scenarios, possible load-balancing strategies may include using weighing to make some nodes likelier to be selected than others. Another scenario could include monitoring the number of requests sent to each node and selecting nodes with lower requests.

## B3
For this lab you will try every node until one succeeds. What flaws do you see in this strategy? What could you do to help in these scenarios?

If certain nodes are disconnected, requests to shards may still try to connect to these nodes as a part of this strategy, which could lead to increases in latency. Therefore, to help in this scenario, the client could store information on which nodes are known to have crashed, and try these nodes less frequently or last. Once the node is connected again, it could have normal precedence or likelihood of being selected. Another issue is that if many nodes are disconnected, it could have high latency as the program tries nodes one-by-one. It may decrease latency if multiple nodes are tried concurrently, as GET is idempotent. However, this would also have a trade-off of increasing the load on the nodes.

## B4
Partial failures could lead to inconsistent replicas (i.e. some of the nodes are missing the new key-value pair or don't properly update the pair). One possible anomaly is that the client may get different results from calling GET, an idempotent function, twice. For instance, it may be directed to one node, which properly updated the key-value pair, the first time, and then another node, which still has the old copy, the second time.

## Group Work

### Gabe
We decided to split the assignment's work according to the parts. I began working on part A server implementation. I worked on this early on so that people working on the Shard Migration wouldn't be blocked from starting their work. Once I finished by portion of the split work, I floated around helping my other project members when they had questions regarding the server I had implemented. We also all worked on the final rigorous tests together. We spent a lot of time trying to debug as to why we were only getting 98% accuracy but after logging both on the server and client end I discovered that the TTLs were being set for over a month. This then led me to discover that the TTLs were being converted to nanoseconds on the client side but interpretted and milliseconds on the server side.

### Alice

### Ben
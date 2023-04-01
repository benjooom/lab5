# Discussions

## A4

The TTL strategy we decided to use involved goroutines running for each stripe in the background and periodically scanning the all the TTLs to determine deletion. One tradeoff to this method is that the value's won't be immediately removed from the cache when they're stale but rather must wait until the goroutine is done sleeping. The goroutine must go through all the entries in the cache and check their TTLs each time which increases overhead. However, this does come with the advantage of having a simpler implementation and being easier to debug issues with TTLs and evictions.

If the server was write intensive then it would be problematic with the current implementation. A read-lock is held while the entire stripe is scanned for stale values which then upgrades to a write-lock when actually deleting them. In a read heavy system, the long read-lock scanning the entire stripe won't cause many latency issues from waiting on locks. However, in a write heavy environment, the relatively long held read-lock could easily clash with the higher volume of write-locks, thus increasing system latency.

## Group Work

### Gabe
We decided to split the assignment's work according to the parts. I began working on part A server implementation. I worked on this early on so that people working on the Shard Migration wouldn't be blocked from starting their work. Once I finished by portion of the split work, I floated around helping my other project members when they had questions regarding the server I had implemented. We also all worked on the final rigorous tests together.

### Alice

### Ben
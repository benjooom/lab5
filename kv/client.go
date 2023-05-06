package kv

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sirupsen/logrus"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

	// Add any client-side state you want here
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	return kv
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	// calculate the appropriate shard, using the function in util.go
	shard := GetShardForKey(key, kv.shardMap.NumShards())

	// Use the provided ShardMap instance in Kv.shardMap to find the set of nodes which host the shard
	possible_nodes := kv.shardMap.NodesForShard(shard)
	var generated_index int
	if len(possible_nodes) > 0 {
		// implement random load balancing (note: random enough?)
		generated_index = rand.Intn(len(possible_nodes))

	} else {
		return "", false, status.Error(codes.NotFound, "Node not available") // TODO: check status code
	}

	// start loop to check nodes, starting from the randomly generated index
	var err error
	for i := 0; i < len(possible_nodes); i++ {
		// Use the provided ClientPool.GetClient to get a KvClient to use to send the request
		// GetClient: returns a KvClient for a given node if one can be created
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil { // For now (though see B3), any errors returned from GetClient or the RPC can be propagated back to the caller as ("", false, err)
			//fmt.Println("client error: ", client_err)
			err = client_err
			continue
			//return "", false, client_err
		}

		// create a GetRequest and send it with KvClient.Get
		out, grpc_err := kvClient.Get(ctx, &proto.GetRequest{Key: key})

		if grpc_err != nil {
			//fmt.Println("grpc error: ", grpc_err)
			err = grpc_err
			continue
			//return "", false, grpc_err
		}

		// will break out of the loop and return the value if the key is found
		return out.Value, out.WasFound, nil
	}
	return "", false, err

}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	// calculate the appropriate shard, using the function in util.go
	shard := GetShardForKey(key, kv.shardMap.NumShards())

	// Use the provided ShardMap instance in Kv.shardMap to find the set of nodes which host the shard
	nodes := kv.shardMap.NodesForShard(shard)

	// Q: error if no nodes found?
	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "Node not available")
	}

	// start loop to check nodes, starting from the randomly generated index
	err := error(nil)
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		// Use the provided ClientPool.GetClient to get a KvClient to use to send the request
		// GetClient: returns a KvClient for a given node if one can be created
		node := nodes[i]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		// concurrent requests to Set (better to make entire section of loop in goroutine?)
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, grpc_err := kvClient.Set(ctx, &proto.SetRequest{Key: key, Value: value, TtlMs: ttl.Milliseconds()})

			if grpc_err != nil {
				err = grpc_err
			}

		}()
	}

	wg.Wait()

	return err

}

//NEW LAB5 FUNCTION: MULTISET

func (kv *Kv) MultiSet(ctx context.Context, keys []string, values []string, ttl time.Duration) (*proto.MultiSetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"keys": keys},
	).Trace("client sending MultiSet() request")
	panic("TODO: Multiset")

	// essential error-checking
	if len(keys) == 0 {
		return &proto.MultiSetResponse{FailedKeys: make([]string, 0)}, status.Error(codes.InvalidArgument, "Must provide at least one key-value pair")
	}
	if len(keys) != len(values) {
		return &proto.MultiSetResponse{FailedKeys: make([]string, 0)}, status.Error(codes.InvalidArgument, "Keys and values must be the same length")
	}
	if ttl <= 0 {
		return &proto.MultiSetResponse{FailedKeys: make([]string, 0)}, status.Error(codes.InvalidArgument, "TTL must be a positive value")
	}

	// calculate the appropriate shards and build map
	failedKeys := KeySet{keys: make([]string, 0)}
	shardDataMap := ShardDataMap{skmap: make(map[int][]KeyValuePair)}

	expiryTime := ttl.Milliseconds()

	var wg sync.WaitGroup

	// first get shards of all the keys asynchronously
	for i := 0; i < len(keys); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := keys[i]
			value := values[i]
			shard := GetShardForKey(key, kv.shardMap.NumShards())

			// add to map
			shardDataMap.mu.Lock()
			if _, ok := shardDataMap.skmap[shard]; !ok {
				shardDataMap.skmap[shard] = make([]KeyValuePair, 0)
				shardDataMap.skmap[shard] = append(shardDataMap.skmap[shard], KeyValuePair{key, value})
			} else {
				shardDataMap.skmap[shard] = append(shardDataMap.skmap[shard], KeyValuePair{key, value})
			}
			shardDataMap.mu.Unlock()

		}(i)
	}
	wg.Wait()

	logrus.WithFields(logrus.Fields{
		"shardDataMap": shardDataMap.skmap},
	).Trace("resulting shard map")
	err := error(nil)

	for shard, data := range shardDataMap.skmap {

		// Use the provided ShardMap instance in Kv.shardMap to find the set of nodes which host the shard
		nodes := kv.shardMap.NodesForShard(shard)

		/* Q: error if no nodes found?
		if len(nodes) == 0 {
			return status.Error(codes.NotFound, "Node not available")
		}*/

		// start loop to check nodes, starting from the randomly generated index

		var wg sync.WaitGroup

		for i := 0; i < len(nodes); i++ {
			// Use the provided ClientPool.GetClient to get a KvClient to use to send the request
			// GetClient: returns a KvClient for a given node if one can be created
			node := nodes[i]
			kvClient, client_err := kv.clientPool.GetClient(node)

			if client_err != nil {
				err = client_err
				continue
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				// Add keys and values to the list
				shard_keys := make([]string, 0)
				shard_values := make([]string, 0)

				for i := 0; i < len(data); i++ {
					shard_keys = append(shard_keys, data[i].Key)
					shard_values = append(shard_values, data[i].Value)
				}

				_, grpc_err := kvClient.MultiSet(ctx, &proto.MultiSetRequest{Key: shard_keys, Value: shard_values, TtlMs: expiryTime})
				if grpc_err != nil {
					failedKeys.mu.Lock()
					failedKeys.keys = append(failedKeys.keys, shard_keys...) // don't set error?
					failedKeys.mu.Unlock()
				}

			}()

			wg.Wait()

		}
	}
	return &proto.MultiSetResponse{FailedKeys: failedKeys.keys}, err
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	// calculate the appropriate shard, using the function in util.go
	shard := GetShardForKey(key, kv.shardMap.NumShards())

	// Use the provided ShardMap instance in Kv.shardMap to find the set of nodes which host the shard
	nodes := kv.shardMap.NodesForShard(shard)

	// Q: error if no nodes found?
	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "Node not available")
	}

	// start loop to check nodes, starting from the randomly generated index
	err := error(nil)
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		// Use the provided ClientPool.GetClient to get a KvClient to use to send the request
		// GetClient: returns a KvClient for a given node if one can be created
		node := nodes[i]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			//fmt.Println("client error: ", client_err)
			err = client_err
			// skip to next node
			continue
		}

		// concurrent requests to Set (better to make entire section of loop in goroutine?)
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, grpc_err := kvClient.Delete(ctx, &proto.DeleteRequest{Key: key})

			if grpc_err != nil {
				err = grpc_err
			}

		}()
	}

	wg.Wait()

	return err

}

func (kv *Kv) CreateList(ctx context.Context, key string, ttl time.Duration) error {

	nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "Node not available")
	}

	err := error(nil)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			mu.Lock()
			err = client_err
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, grpc_err := kvClient.CreateList(ctx, &proto.CreateListRequest{Key: key, TtlMs: ttl.Milliseconds()})
			if grpc_err != nil {
				mu.Lock()
				err = grpc_err
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return err
}

func (kv *Kv) CreateSet(ctx context.Context, key string, ttl time.Duration) error {

	nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "Node not available")
	}

	err := error(nil)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			mu.Lock()
			err = client_err
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, grpc_err := kvClient.CreateSet(ctx, &proto.CreateSetRequest{Key: key, TtlMs: ttl.Milliseconds()})
			if grpc_err != nil {
				mu.Lock()
				err = grpc_err
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return err
}

func (kv *Kv) CreateSortedSet(ctx context.Context, key string, ttl time.Duration) error {

	nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "Node not available")
	}

	err := error(nil)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			mu.Lock()
			err = client_err
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, grpc_err := kvClient.CreateSortedSet(ctx, &proto.CreateSortedSetRequest{Key: key, TtlMs: ttl.Milliseconds()})
			if grpc_err != nil {
				mu.Lock()
				err = grpc_err
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return err
}

func (kv *Kv) AppendList(ctx context.Context, key string, value string) error {

	nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "Node not available")
	}

	err := error(nil)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			mu.Lock()
			err = client_err
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, grpc_err := kvClient.AppendList(ctx, &proto.AppendListRequest{Key: key, Value: value})
			if grpc_err != nil {
				mu.Lock()
				err = grpc_err
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return err
}

func (kv *Kv) AppendSet(ctx context.Context, key string, value string) error {

	nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "Node not available")
	}

	err := error(nil)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			mu.Lock()
			err = client_err
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, grpc_err := kvClient.AppendSet(ctx, &proto.AppendSetRequest{Key: key, Value: value})
			if grpc_err != nil {
				mu.Lock()
				err = grpc_err
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return err
}

func (kv *Kv) AppendSortedSet(ctx context.Context, key string, value string, rank int64) error {

	nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "Node not available")
	}

	err := error(nil)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			mu.Lock()
			err = client_err
			mu.Unlock()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_, grpc_err := kvClient.AppendSortedSet(ctx, &proto.AppendSortedSetRequest{Key: key, Value: value, Rank: rank})
			if grpc_err != nil {
				mu.Lock()
				err = grpc_err
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return err
}

func (kv *Kv) PopList(ctx context.Context, key string) (string, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return "", status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.PopList(ctx, &proto.PopListRequest{Key: key})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.Value, nil
	}
	return "", err
}

func (kv *Kv) RemoveList(ctx context.Context, key string, value string) (bool, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return false, status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.RemoveList(ctx, &proto.RemoveListRequest{Key: key, Value: value})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.Status, nil
	}
	return false, err

}

func (kv *Kv) RemoveSet(ctx context.Context, key string, value string) (bool, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return false, status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.RemoveSet(ctx, &proto.RemoveSetRequest{Key: key, Value: value})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.Status, nil
	}
	return false, err
}

func (kv *Kv) RemoveSortedSet(ctx context.Context, key string, value string) (bool, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return false, status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.RemoveSortedSet(ctx, &proto.RemoveSortedSetRequest{Key: key, Value: value})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.Status, nil
	}
	return false, err
}

func (kv *Kv) CheckList(ctx context.Context, key string, value string) (bool, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return false, status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.CheckList(ctx, &proto.CheckListRequest{Key: key, Value: value})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.Status, nil
	}
	return false, err

}

func (kv *Kv) CheckSet(ctx context.Context, key string, value string) (bool, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return false, status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.CheckSet(ctx, &proto.CheckSetRequest{Key: key, Value: value})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.Status, nil
	}
	return false, err

}

func (kv *Kv) CheckSortedSet(ctx context.Context, key string, value string) (bool, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return false, status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.CheckSortedSet(ctx, &proto.CheckSortedSetRequest{Key: key, Value: value})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.Status, nil
	}
	return false, err

}

func (kv *Kv) CAS(ctx context.Context, key string, value string, expected string, ttl time.Duration) (bool, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return false, status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.CAS(ctx, &proto.CASRequest{Key: key, Value: value, Expected: expected, TtlMs: ttl.Milliseconds()})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.WasSet, nil
	}
	return false, err

}

func (kv *Kv) GetRange(ctx context.Context, key string, start int64, end int64) ([]string, error) {

	possible_nodes := kv.shardMap.NodesForShard(GetShardForKey(key, kv.shardMap.NumShards()))

	var generated_index int
	if len(possible_nodes) > 0 {
		generated_index = rand.Intn(len(possible_nodes))
	} else {
		return nil, status.Error(codes.NotFound, "Node not available")
	}

	var err error = nil
	for i := 0; i < len(possible_nodes); i++ {
		node := possible_nodes[(generated_index+i)%len(possible_nodes)]
		kvClient, client_err := kv.clientPool.GetClient(node)

		if client_err != nil {
			err = client_err
			continue
		}

		out, grpc_err := kvClient.GetRange(ctx, &proto.GetRangeRequest{Key: key, Start: start, End: end})

		if grpc_err != nil {
			err = grpc_err
			continue
		}

		return out.Values, nil
	}
	return nil, err

}

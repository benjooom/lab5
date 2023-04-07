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

	//panic("TODO: Part B")
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
			//fmt.Println("client error: ", client_err)
			err = client_err
			continue
			//return "", false, client_err
		}

		// concurrent requests to Set (better to make entire section of loop in goroutine?)
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, grpc_err := kvClient.Set(ctx, &proto.SetRequest{Key: key, Value: value, TtlMs: int64(ttl)})

			if grpc_err != nil {
				err = grpc_err
			}

		}()
	}

	wg.Wait()

	return err

	//panic("TODO: Part B")
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

	//panic("TODO: Part B")
}

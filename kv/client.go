package kv

import (
	"context"
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
	var node string
	if len(possible_nodes) > 0 {
		// for now, just pick the first node
		node = possible_nodes[0]
	} else {
		return "", false, status.Error(codes.NotFound, "Node not available") // TODO: check status code
	}

	// Use the provided ClientPool.GetClient to get a KvClient to use to send the request
	// GetClient: returns a KvClient for a given node if one can be created
	kvClient, client_err := kv.clientPool.GetClient(node)

	if client_err != nil { // For now (though see B3), any errors returned from GetClient or the RPC can be propagated back to the caller as ("", false, err)
		return "", false, client_err
	}

	// create a GetRequest and send it with KvClient.Get
	out, grpc_err := kvClient.Get(ctx, &proto.GetRequest{Key: key})

	if grpc_err != nil {
		return "", false, grpc_err
	}

	return out.Value, out.WasFound, nil

	//panic("TODO: Part B")
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	panic("TODO: Part B")
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	panic("TODO: Part B")
}

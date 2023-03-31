package kv

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"

	"golang.org/x/exp/slices"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	database KvServerState
}

// Stores all of the state information (stripes)
type KvServerState struct {
	stripes []*Stripe
}

// Stores varies entries under a lock
type Stripe struct {
	mutex sync.RWMutex
	state map[string]Entry
}

// Basic value object (value and ttl)
type Entry struct {
	value string
	ttl   int64
}

func makeKvServerState(size int) KvServerState {
	stripes := make([]*Stripe, size)
	for i := 0; i < size; i++ {
		stripes[i] = &Stripe{mutex: sync.RWMutex{}, state: make(map[string]Entry)}
	}
	return KvServerState{stripes: stripes}
}

// Set a key in the database.
// Returns true if the key was set, false otherwise.
func (database *KvServerState) Set(key string, value string, ttl int64) bool {
	hash, err := fnv.New64().Write([]byte(key))
	if err != nil {
		return false
	}
	stripe := database.stripes[hash%len(database.stripes)]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()
	stripe.state[key] = Entry{value, ttl}
	return true
}

// Gets a key from the database.
// Returns the value and true if the key was present, false otherwise.
func (database *KvServerState) Get(key string) (string, bool) {
	hash, err := fnv.New64().Write([]byte(key))
	if err != nil {
		return "", false
	}
	stripe := database.stripes[hash%len(database.stripes)]
	stripe.mutex.RLock()
	defer stripe.mutex.RUnlock()
	entry, ok := stripe.state[key]
	if !ok {
		return "", false
	}
	return entry.value, true
}

// Deletes a key from the database.
// Returns true if the key was present, false otherwise.
func (database *KvServerState) Delete(key string) bool {
	hash, err := fnv.New64().Write([]byte(key))
	if err != nil {
		return false
	}
	stripe := database.stripes[hash%len(database.stripes)]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()
	_, ok := stripe.state[key]
	if !ok {
		return false
	}
	delete(stripe.state, key)
	return true
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()

	// Number of stripes in the database. You may change this value.
	stripeCount := 10

	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		database:   makeKvServerState(stripeCount),
	}
	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	return &server
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	server.listener.Close()
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	// panic("TODO: Part A")

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	// If the shard is not in the nodes' covered shards, error
	if !slices.Contains(server.shardMap.ShardsForNode(server.nodeName), shard) {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.NotFound, "Incorrect shard")
	}

	if request.Key == "" {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	entry, ok := server.database.Get(request.Key)
	if !ok {
		return &proto.GetResponse{Value: "", WasFound: false}, nil
	}
	return &proto.GetResponse{Value: entry, WasFound: true}, nil

}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	// panic("TODO: Part A")

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	// If the shard is not in the nodes' covered shards, error
	if !slices.Contains(server.shardMap.ShardsForNode(server.nodeName), shard) {
		return &proto.SetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}

	// SPEC NOTE: "Empty keys are not allowed (error with INVALID_ARGUMENT)."
	if request.Key == "" {
		return &proto.SetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	ok := server.database.Set(request.Key, request.Value, request.TtlMs)
	if !ok {
		return &proto.SetResponse{}, errors.New("InternalError Failed to set key")
	}
	return &proto.SetResponse{}, nil

}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	// panic("TODO: Part A")

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	// If the shard is not in the nodes' covered shards, error
	if !slices.Contains(server.shardMap.ShardsForNode(server.nodeName), shard) {
		return &proto.DeleteResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}

	if request.Key == "" {
		return &proto.DeleteResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	ok := server.database.Delete(request.Key)
	if !ok {
		// Should never happen
		return &proto.DeleteResponse{}, nil
	}
	return &proto.DeleteResponse{}, nil

}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	panic("TODO: Part C")
}

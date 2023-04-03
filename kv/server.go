package kv

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

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

	mySL     sync.RWMutex
	myShards []int
}

// Stores all of the state information (stripes)
type KvServerState struct {
	killed  int32
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

	// Initialize all stripes
	stripes := make([]*Stripe, size)
	for i := 0; i < size; i++ {
		stripes[i] = &Stripe{mutex: sync.RWMutex{}, state: make(map[string]Entry)}
	}

	state := KvServerState{stripes: stripes, killed: 0}

	for _, stripe := range stripes {
		// Async function to remove expired keys (ttl management)
		go func(stripe *Stripe, killed *int32) {

			// Loop through entire stripe every second (stop when killed)
			for atomic.LoadInt32(killed) == 0 {

				// Sleep for 1 second
				time.Sleep(time.Millisecond * 1000)

				// Read through entire stripe and find expired keys
				expiredKeys := make([]string, 0)
				stripe.mutex.RLock()
				for key, entry := range stripe.state {
					if time.Now().UnixMilli() > entry.ttl {
						expiredKeys = append(expiredKeys, key)
					}
				}
				stripe.mutex.RUnlock()

				// Loop through all expired keys and delete them
				stripe.mutex.Lock()
				for _, key := range expiredKeys {
					// Confirm that the key is still expired
					// (it may have been updated while we were waiting for the lock)
					if time.Now().UnixMilli() > stripe.state[key].ttl {
						delete(stripe.state, key)
					}
				}
				stripe.mutex.Unlock()
			}
		}(stripe, &state.killed)
	}

	return state
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
	stripe.state[key] = Entry{value, time.Now().UnixMilli() + ttl}
	return true
}

// Gets a key from the database.
// Returns the value and true if the key was present, false otherwise.
func (database *KvServerState) Get(key string) (string, bool) {

	// Get the stripe
	hash, err := fnv.New64().Write([]byte(key))
	if err != nil {
		return "", false
	}
	stripe := database.stripes[hash%len(database.stripes)]

	stripe.mutex.RLock()
	defer stripe.mutex.RUnlock()

	// Key not present
	entry, ok := stripe.state[key]
	if !ok {
		return "", false
	}

	// Expired data
	if time.Now().UnixMilli() > entry.ttl {
		return "", false
	}

	// Key present, not expired
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
	// Lock reading myShards
	server.mySL.Lock()
	defer server.mySL.Unlock()

	// Iterate through all shards in the shard map and find the ones that are assigned to this node
	shards := server.shardMap.ShardsForNode(server.nodeName)
	toRemove := make([]int, 0)
	toAdd := make([]int, 0)
	for _, shard := range shards {
		if !slices.Contains(server.myShards, shard) {
			toAdd = append(toAdd, shard)
		} else {
			toRemove = append(toRemove, shard)
		}
	}

	// Iterate through database and remove all keys that are in shards that are no longer assigned to this node
	numShards := server.shardMap.NumShards()
	for i := range server.database.stripes {
		for key := range server.database.stripes[i].state {
			if slices.Contains(toRemove, GetShardForKey(key, numShards)) {
				server.database.Delete(key)
			}
		}
	}

	server.myShards = shards
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			// Will stop async ttl management go routines
			atomic.AddInt32(&server.database.killed, 1)
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
		myShards:   make([]int, 0),
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
	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

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
	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.SetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

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
	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.DeleteResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

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
	//panic("TODO: Part C")

	// If the shard is not in the nodes' covered shards, error
	server.mySL.RLock()
	if !slices.Contains(server.myShards, int(request.Shard)) {
		server.mySL.RUnlock()
		return &proto.GetShardContentsResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	values := make([]*proto.GetShardValue, 0)
	// Check the database for values assigned to a shard
	numShards := server.shardMap.NumShards()
	for i := range server.database.stripes {
		for key := range server.database.stripes[i].state {
			if GetShardForKey(key, numShards) == int(request.Shard) {
				remainingTime := server.database.stripes[i].state[key].ttl - time.Now().UnixMilli()
				newValue := &proto.GetShardValue{Key: key,
					Value:          server.database.stripes[i].state[key].value,
					TtlMsRemaining: remainingTime}
				// Add to list of values
				values = append(values, newValue)
			}
		}
	}

	return &proto.GetShardContentsResponse{Values: values}, nil
}

package kv

import (
	"context"
	"errors"
	"math/rand"
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
	rpcMutex sync.RWMutex
	myShards []int
}

// Stores all of the state information (stripes)
type KvServerState struct {
	killed  int32
	stripes map[int]*Stripe // shard int mapping to stripe
}

// Stores varies entries under a lock
type Stripe struct {
	mutex sync.RWMutex
	state map[string]Entry
}

// Basic value object (value and ttl)
type Entry struct {
	value string
	ttl   time.Time
}

func makeKvServerState(size int) KvServerState {

	// Initialize all stripes
	stripes := make(map[int]*Stripe, size)
	for i := 0; i < size; i++ {
		// +1 bc shards are 1-indexed
		stripes[i+1] = &Stripe{mutex: sync.RWMutex{}, state: make(map[string]Entry)}
	}

	state := KvServerState{stripes: stripes, killed: 0}

	for _, stripe := range stripes {
		// Async function to remove expired keys (ttl management)
		go func(stripe *Stripe, kill *int32) {

			// Loop through entire stripe every second (stop when killed)
			for atomic.LoadInt32(kill) == 0 {

				// Sleep for 1 second
				time.Sleep(time.Millisecond * 1000)

				// Read through entire stripe and find expired keys
				expiredKeys := make([]string, 0)
				stripe.mutex.RLock()
				for key, entry := range stripe.state {
					// Check if entry.ttl is before now
					if entry.ttl.Before(time.Now()) {
						expiredKeys = append(expiredKeys, key)
					}
				}
				stripe.mutex.RUnlock()

				// Nothing to delete, go back to sleep!
				if len(expiredKeys) == 0 {
					continue
				}

				// Loop through all expired keys and delete them
				stripe.mutex.Lock()
				for _, key := range expiredKeys {
					// Confirm that the key is still expired
					// (it may have been updated while we were waiting for the lock)
					if stripe.state[key].ttl.Before(time.Now()) {
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
func (database *KvServerState) Set(key string, value string, expireyTime time.Time, shard int) bool {

	// Get the stripe
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()
	// Set expirey time as a time object
	stripe.state[key] = Entry{value, expireyTime}
	return true
}

// Gets a key from the database.
// Returns the value and true if the key was present, false otherwise.
func (database *KvServerState) Get(key string, shard int) (string, bool) {

	// Get the stripe
	stripe := database.stripes[shard]

	stripe.mutex.RLock()
	defer stripe.mutex.RUnlock()

	// Key not present
	entry, ok := stripe.state[key]
	if !ok {
		return "", false
	}

	// Expired data
	if entry.ttl.Before(time.Now()) {
		return "", false
	}

	// Key present, not expired
	return entry.value, true
}

// Deletes a key from the database.
// Returns true if the key was present, false otherwise.
func (database *KvServerState) Delete(key string, shard int) bool {

	// Get the stripe
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()
	_, ok := stripe.state[key]
	if !ok {
		return false
	}
	delete(stripe.state, key)
	return true
}

func (server *KvServerImpl) copyShardData(toAdd []int) {

	// Create a map of all shards toAdd to their current owners
	shardMap := make(map[int][]string)
	for i := 0; i < len(toAdd); i++ {
		shardMap[toAdd[i]] = server.shardMap.NodesForShard(toAdd[i])
	}

	clientMap := make(map[string]proto.KvClient)
	for shard, nodes := range shardMap {
		// Create a client for each owner if it doesn't already exist
		for _, node := range nodes {
			// Make sure owner is not this node
			if node == server.nodeName {
				continue
			}

			// Create a client if it doesn't already exist
			if _, ok := clientMap[node]; !ok {
				client, err := server.clientPool.GetClient(node)
				if err != nil {
					continue
				}
				clientMap[node] = client
			}
		}

		// Create a list of owners and shuffle it
		owners := make([]string, len(nodes))
		copy(owners, nodes)
		rand.Shuffle(len(owners), func(i, j int) { owners[i], owners[j] = owners[j], owners[i] })

		// While there is an error, keep trying to get data from owners
		for index := len(owners) - 1; index >= 0; index-- {

			// Retrieve a client from the clientMap at index if it exists
			_, ok := clientMap[owners[index]]
			if !ok {
				continue
			}

			// Use client to GetShardContents
			// Create a context with a timeout of 2 second
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Get the shard contents from the owner
			response, err := clientMap[owners[index]].GetShardContents(ctx, &proto.GetShardContentsRequest{Shard: int32(shard)})
			if err != nil {
				continue
			}

			stripe := server.database.stripes[shard]
			stripe.mutex.Lock()

			// Wipe out the previous data in the stripe
			stripe.state = make(map[string]Entry)

			// Add all the new data to the stripe
			for i := range response.Values {
				stripe.state[response.Values[i].Key] = Entry{response.Values[i].Value, time.Now().Add(time.Millisecond * time.Duration(response.Values[i].TtlMsRemaining))}
			}
			stripe.mutex.Unlock()

			break
		}
	}
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
	// Prevent any other RPC, except for GetShardContents
	server.rpcMutex.Lock()
	defer server.rpcMutex.Unlock()

	// Get the shards that are assigned to this node and updated shards
	server.mySL.RLock()
	myShards := server.myShards
	updatedShards := server.shardMap.ShardsForNode(server.nodeName)
	server.mySL.RUnlock()

	// Iterate through all shards in the shard map and find the ones that are assigned to this node
	toRemove := make([]int, 0)
	for _, shard := range myShards {
		if !slices.Contains(updatedShards, shard) { // Shard is no longer assigned to this node
			toRemove = append(toRemove, shard)
		}
	}
	toAdd := make([]int, 0)
	for _, shard := range updatedShards {
		if !slices.Contains(myShards, shard) { // Shard is newly assigned to this node
			toAdd = append(toAdd, shard)
		}
	}

	server.mySL.Lock()
	server.myShards = difference(server.myShards, toRemove)
	server.mySL.Unlock()
	// Empty out all stripes/shards that are no longer assigned to this node
	for _, shard := range toRemove {
		stripe := server.database.stripes[shard]
		stripe.mutex.Lock()
		stripe.state = make(map[string]Entry)
		stripe.mutex.Unlock()
	}

	// If there are shards to add, copy the data from the owners
	if len(toAdd) != 0 {
		server.copyShardData(toAdd)
	}

	server.mySL.Lock()
	server.myShards = updatedShards
	server.mySL.Unlock()

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
	stripeCount := shardMap.NumShards()

	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		database:   makeKvServerState(stripeCount),
		myShards:   make([]int, 0),
		mySL:       sync.RWMutex{},
		rpcMutex:   sync.RWMutex{},
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

	if request.Key == "" {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	// panic("TODO: Part A")
	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()
	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	// If the shard is not in the nodes' covered shards, error
	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	entry, ok := server.database.Get(request.Key, shard)
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

	// SPEC NOTE: "Empty keys are not allowed (error with INVALID_ARGUMENT)."
	if request.Key == "" {
		return &proto.SetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	expireyTime := time.Now().Add(time.Millisecond * time.Duration(request.TtlMs))

	// panic("TODO: Part A")
	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()
	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	// If the shard is not in the nodes' covered shards, error
	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.SetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.database.Set(request.Key, request.Value, expireyTime, shard)
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

	if request.Key == "" {
		return &proto.DeleteResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	// panic("TODO: Part A")
	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()
	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	if request.Key == "" {
		return &proto.DeleteResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	// If the shard is not in the nodes' covered shards, error
	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.DeleteResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.database.Delete(request.Key, shard)
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

	server.mySL.RLock()
	defer server.mySL.RUnlock()
	if !slices.Contains(server.myShards, int(request.Shard)) {
		return &proto.GetShardContentsResponse{}, status.Error(codes.NotFound, "Shard not found")
	}

	// Initialize necessary variables
	values := make([]*proto.GetShardValue, 0)

	stripe := server.database.stripes[int(request.Shard)]
	stripe.mutex.Lock()
	for key := range stripe.state {
		newValue := &proto.GetShardValue{
			Key:            key,
			Value:          stripe.state[key].value,
			TtlMsRemaining: time.Until(stripe.state[key].ttl).Milliseconds(),
		}
		values = append(values, newValue)
	}
	stripe.mutex.Unlock()

	return &proto.GetShardContentsResponse{Values: values}, nil
}

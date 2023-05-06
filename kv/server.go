package kv

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/slices"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"github.com/wangjia184/sortedset"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mapping of datatype to database slice
var typeMap = map[string]int{
	"STRING":    0,
	"LIST":      1,
	"SET":       2,
	"SORTEDSET": 3,
}

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	database []KvServerState

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

// // Basic value object (value and ttl)
type Entry interface {
	GetExpiry() time.Time
	GetValue() interface{}
}

type String struct {
	value string
	ttl   time.Time
}

func (s String) GetExpiry() time.Time {
	return s.ttl
}

func (s String) GetValue() interface{} {
	return s.value
}

type List struct {
	value []string
	ttl   time.Time
}

func (s List) GetExpiry() time.Time {
	return s.ttl
}

func (s List) GetValue() interface{} {
	return s.value
}

type Set struct {
	value map[string]bool
	ttl   time.Time
}

func (s Set) GetExpiry() time.Time {
	return s.ttl
}

func (s Set) GetValue() interface{} {
	return s.value
}

type SortedSet struct {
	value sortedset.SortedSet
	ttl   time.Time
}

func (s SortedSet) GetExpiry() time.Time {
	return s.ttl
}

func (s SortedSet) GetValue() interface{} {
	return s.value
}

// types for MultiSet
type KeySet struct {
	mu   sync.RWMutex
	keys []string
}

/*
type KeyShardMap struct {
	mu    sync.RWMutex
	ksmap map[string]int
}*/

type ShardDataMap struct {
	mu    sync.RWMutex
	skmap map[int][]KeyValuePair
}

type KeyValuePair struct {
	Key   string
	Value string
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
					if entry.GetExpiry().Before(time.Now()) {
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
					if stripe.state[key].GetExpiry().Before(time.Now()) {
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
	stripe.state[key] = String{value, expireyTime}
	return true
}

// NEW LAB 5 FUNCTION: MULTISET
func (database *KvServerState) MultiSet(key []string, value []string, expiryTime time.Time, shard int) bool {
	// This function will only ever be called by MultiSet, which will ensure the following invariants:
	// Invariant 1: all strings will belong to the same shard, providing per-shard atomicity
	// Invariant 2: all keys will be unique, providing per-key atomicity

	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()
	// Set expiry time as a time object
	for i := 0; i < len(key); i++ {
		stripe.state[key[i]] = String{value[i], expiryTime}
	}

	return true
}

func (database *KvServerState) CAS(key string, value string, expected string, expiryTime time.Time, shard int) bool {

	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	// check if key in database
	if _, ok := stripe.state[key]; !ok {
		return false
	}

	// check if value matches expected
	if stripe.state[key].GetValue().(string) == expected {
		stripe.state[key] = String{value, expiryTime}
		return true
	}

	return false
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
	if entry.GetExpiry().Before(time.Now()) {
		return "", false
	}

	// Key present, not expired
	return entry.GetValue().(string), true
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

			stripe := server.GetStringDB().stripes[shard]
			stripe.mutex.Lock()

			// Wipe out the previous data in the stripe
			stripe.state = make(map[string]Entry)

			// Add all the new data to the stripe
			for i := range response.Values {
				stripe.state[response.Values[i].Key] = String{response.Values[i].Value, time.Now().Add(time.Millisecond * time.Duration(response.Values[i].TtlMsRemaining))}
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
		stripe := server.GetStringDB().stripes[shard]
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
			// TODO: Updated the killed for all kvstates "STRING", "LIST", "SET", "SORTEDSET"
			atomic.AddInt32(&server.GetStringDB().killed, 1)
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func (server *KvServerImpl) GetStringDB() *KvServerState {
	return &server.database[typeMap["STRING"]]
}

func (server *KvServerImpl) GetListDB() *KvServerState {
	return &server.database[typeMap["LIST"]]
}

func (server *KvServerImpl) GetSetDB() *KvServerState {
	return &server.database[typeMap["SET"]]
}

func (server *KvServerImpl) GetSortedSetDB() *KvServerState {
	return &server.database[typeMap["SORTEDSET"]]
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()

	// Number of stripes in the database. You may change this value.
	stripeCount := shardMap.NumShards()

	// Create a database for each datatype
	database := make([]KvServerState, len(typeMap))
	for _, v := range typeMap {
		database[v] = makeKvServerState(stripeCount)
	}

	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		database:   database,
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

	entry, ok := server.GetStringDB().Get(request.Key, shard)
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

	ok := server.GetStringDB().Set(request.Key, request.Value, expireyTime, shard)
	if !ok {
		return &proto.SetResponse{}, errors.New("InternalError Failed to set key")
	}
	return &proto.SetResponse{}, nil

}

// LAB 5 NEW FUNCTION: MULTISET & CAS

func (server *KvServerImpl) MultiSet(
	ctx context.Context,
	request *proto.MultiSetRequest,
) (*proto.MultiSetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received MultiSet() request")

	// error-checking (doesn't handle duplicates)
	if len(request.Key) == 0 {
		return &proto.MultiSetResponse{}, status.Error(codes.InvalidArgument, "Must provide at least one key-value pair")
	}
	if len(request.Key) != len(request.Value) {
		return &proto.MultiSetResponse{}, status.Error(codes.InvalidArgument, "Keys and values must be the same length")
	}
	if request.TtlMs <= 0 {
		return &proto.MultiSetResponse{}, status.Error(codes.InvalidArgument, "TTL must be a positive value")
	}
	failedKeys := KeySet{}
	shardDataMap := ShardDataMap{}

	expiryTime := time.Now().Add(time.Millisecond * time.Duration(request.TtlMs))

	// instantaneous locking and unlocking to ensure finish updates?
	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	var wg sync.WaitGroup

	// first get shards of all the keys asynchronously
	for i := 0; i < len(request.Key); i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := request.Key[i]
			value := request.Value[i]
			shard := GetShardForKey(key, server.shardMap.NumShards())

			// If the shard is not in the nodes' covered shards, key has "failed"
			server.mySL.RLock()
			if !slices.Contains(server.myShards, shard) {
				server.mySL.RUnlock()

				// Update list of failed keys appropriately
				failedKeys.mu.Lock()
				failedKeys.keys = append(failedKeys.keys, key) // don't set error?
				failedKeys.mu.Unlock()
			} else {
				server.mySL.RUnlock()

				// add to map
				shardDataMap.mu.Lock()
				if _, ok := shardDataMap.skmap[shard]; !ok {
					shardDataMap.skmap[shard] = make([]KeyValuePair, 0)
					shardDataMap.skmap[shard] = append(shardDataMap.skmap[shard], KeyValuePair{key, value})
				} else {
					shardDataMap.skmap[shard] = append(shardDataMap.skmap[shard], KeyValuePair{key, value})
				}
				shardDataMap.mu.Unlock()

			}

		}(i)
	}
	wg.Wait()

	for shard, data := range shardDataMap.skmap {
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

			ok := server.GetStringDB().MultiSet(shard_keys, shard_values, expiryTime, shard)
			if !ok {
				failedKeys.mu.Lock()
				failedKeys.keys = append(failedKeys.keys, shard_keys...) // don't set error?
				failedKeys.mu.Unlock()
			}

		}()

		wg.Wait()

	}

	// return
	return &proto.MultiSetResponse{FailedKeys: failedKeys.keys}, nil

}

func (server *KvServerImpl) CAS(
	ctx context.Context,
	request *proto.CASRequest,
) (*proto.CASResponse, error) {

	if request.Key == "" {
		return &proto.CASResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	expireyTime := time.Now().Add(time.Millisecond * time.Duration(request.TtlMs))

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()
	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.CASResponse{WasSet: false}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.GetStringDB().CAS(request.Key, request.Value, request.Expected, expireyTime, shard)

	return &proto.CASResponse{WasSet: ok}, nil

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

	ok := server.GetStringDB().Delete(request.Key, shard)
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

	stripe := server.GetStringDB().stripes[int(request.Shard)]
	stripe.mutex.Lock()
	for key := range stripe.state {
		// If the key is expired, don't copy it
		newValue := &proto.GetShardValue{
			Key:            key,
			Value:          stripe.state[key].GetValue().(string),
			TtlMsRemaining: time.Until(stripe.state[key].GetExpiry()).Milliseconds(),
		}
		values = append(values, newValue)
	}
	stripe.mutex.Unlock()

	return &proto.GetShardContentsResponse{Values: values}, nil
}

// LAB 5 NEW FUNCTIONS:

func (server *KvServerImpl) CreateList(
	ctx context.Context,
	request *proto.CreateListRequest,
) (*proto.CreateListResponse, error) {

	if request.Key == "" {
		return &proto.CreateListResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	expireyTime := time.Now().Add(time.Millisecond * time.Duration(request.TtlMs))

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.CreateListResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.GetListDB().CreateList(request.Key, expireyTime, shard)
	if !ok {
		return &proto.CreateListResponse{}, status.Error(codes.Internal, "InternalError Failed to create list")
	}
	return &proto.CreateListResponse{}, nil

}

func (database *KvServerState) CreateList(key string, expireyTime time.Time, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()
	// Set expirey time as a time object
	stripe.state[key] = List{make([]string, 0), expireyTime}
	return true
}

func (server *KvServerImpl) CreateSet(
	ctx context.Context,
	request *proto.CreateSetRequest,
) (*proto.CreateSetResponse, error) {

	if request.Key == "" {
		return &proto.CreateSetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	expireyTime := time.Now().Add(time.Millisecond * time.Duration(request.TtlMs))

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.CreateSetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.GetSetDB().CreateSet(request.Key, expireyTime, shard)
	if !ok {
		return &proto.CreateSetResponse{}, status.Error(codes.Internal, "InternalError Failed to create set")
	}

	return &proto.CreateSetResponse{}, nil
}

func (database *KvServerState) CreateSet(key string, expireyTime time.Time, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()
	// Set expirey time as a time object
	stripe.state[key] = Set{make(map[string]bool), expireyTime}
	return true
}

func (server *KvServerImpl) CreateSortedSet(
	ctx context.Context,
	request *proto.CreateSortedSetRequest,
) (*proto.CreateSortedSetResponse, error) {

	if request.Key == "" {
		return &proto.CreateSortedSetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	expireyTime := time.Now().Add(time.Millisecond * time.Duration(request.TtlMs))

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.CreateSortedSetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.GetSortedSetDB().CreateSortedSet(request.Key, expireyTime, shard)
	if !ok {
		return &proto.CreateSortedSetResponse{}, status.Error(codes.Internal, "InternalError Failed to create sorted set")
	}

	return &proto.CreateSortedSetResponse{}, nil
}

func (database *KvServerState) CreateSortedSet(key string, expireyTime time.Time, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()
	stripe.state[key] = SortedSet{*sortedset.New(), expireyTime}
	return true
}

func (server *KvServerImpl) AppendList(
	ctx context.Context,
	request *proto.AppendListRequest,
) (*proto.AppendListResponse, error) {

	if request.Key == "" {
		return &proto.AppendListResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.AppendListResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.GetListDB().AppendList(request.Key, request.Value, shard)
	if !ok {
		return &proto.AppendListResponse{}, status.Error(codes.Internal, "InternalError Failed to insert to list")
	}

	return &proto.AppendListResponse{}, nil
}

func (database *KvServerState) AppendList(key string, value string, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	entry := stripe.state[key].(List)
	entry.value = append(entry.value, value)
	stripe.state[key] = entry

	return true
}

func (server *KvServerImpl) AppendSet(
	ctx context.Context,
	request *proto.AppendSetRequest,
) (*proto.AppendSetResponse, error) {

	if request.Key == "" {
		return &proto.AppendSetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.AppendSetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.GetSetDB().AppendSet(request.Key, request.Value, shard)
	if !ok {
		return &proto.AppendSetResponse{}, status.Error(codes.Internal, "InternalError Failed to insert to list")
	}

	return &proto.AppendSetResponse{}, nil
}

func (database *KvServerState) AppendSet(key string, value string, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	entry := stripe.state[key].(Set)
	entry.value[value] = true
	stripe.state[key] = entry

	return true
}

func (server *KvServerImpl) AppendSortedSet(
	ctx context.Context,
	request *proto.AppendSortedSetRequest,
) (*proto.AppendSortedSetResponse, error) {

	if request.Key == "" {
		return &proto.AppendSortedSetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.AppendSortedSetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	ok := server.GetSortedSetDB().AppendSortedSet(request.Key, request.Value, request.Rank, shard)
	if !ok {
		return &proto.AppendSortedSetResponse{}, status.Error(codes.Internal, "InternalError Failed to insert to sorted set")
	}

	return &proto.AppendSortedSetResponse{}, nil
}

func (database *KvServerState) AppendSortedSet(key string, value string, rank int64, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	entry := stripe.state[key].(SortedSet)
	status := entry.value.AddOrUpdate(value, sortedset.SCORE(rank), interface{}(nil))
	stripe.state[key] = entry

	return status
}

func (server *KvServerImpl) RemoveList(
	ctx context.Context,
	request *proto.RemoveListRequest,
) (*proto.RemoveListResponse, error) {

	if request.Key == "" {
		return &proto.RemoveListResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.RemoveListResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	found := server.GetListDB().RemoveList(request.Key, request.Value, shard)

	return &proto.RemoveListResponse{Status: found}, nil
}

func (database *KvServerState) RemoveList(key string, value string, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	entry := stripe.state[key].(List)
	for i, v := range entry.value {
		if v == value {
			entry.value = append(entry.value[:i], entry.value[i+1:]...)
			stripe.state[key] = entry
			return true
		}
	}

	return false
}

func (server *KvServerImpl) RemoveSet(
	ctx context.Context,
	request *proto.RemoveSetRequest,
) (*proto.RemoveSetResponse, error) {

	if request.Key == "" {
		return &proto.RemoveSetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.RemoveSetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	found := server.GetSetDB().RemoveSet(request.Key, request.Value, shard)

	return &proto.RemoveSetResponse{Status: found}, nil
}

func (database *KvServerState) RemoveSet(key string, value string, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	entry := stripe.state[key].(Set)
	delete(entry.value, value)
	stripe.state[key] = entry

	return true
}

func (server *KvServerImpl) RemoveSortedSet(
	ctx context.Context,
	request *proto.RemoveSortedSetRequest,
) (*proto.RemoveSortedSetResponse, error) {

	if request.Key == "" {
		return &proto.RemoveSortedSetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.RemoveSortedSetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	found := server.GetSortedSetDB().RemoveSortedSet(request.Key, request.Value, shard)

	return &proto.RemoveSortedSetResponse{Status: found}, nil
}

func (database *KvServerState) RemoveSortedSet(key string, value string, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	entry := stripe.state[key].(SortedSet)
	entry.value.Remove(value)
	stripe.state[key] = entry

	return true
}

func (server *KvServerImpl) CheckList(
	ctx context.Context,
	request *proto.CheckListRequest,
) (*proto.CheckListResponse, error) {

	if request.Key == "" {
		return &proto.CheckListResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.CheckListResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	found := server.GetListDB().CheckList(request.Key, request.Value, shard)

	return &proto.CheckListResponse{Status: found}, nil
}

func (database *KvServerState) CheckList(key string, value string, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	// NOTE: change this when sorted list!
	entry := stripe.state[key].(List)

	// print out the list
	fmt.Println(entry.value)

	for _, v := range entry.value {
		if v == value {
			return true
		}
	}

	return false
}

func (server *KvServerImpl) CheckSet(
	ctx context.Context,
	request *proto.CheckSetRequest,
) (*proto.CheckSetResponse, error) {

	if request.Key == "" {
		return &proto.CheckSetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.CheckSetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	found := server.GetSetDB().CheckSet(request.Key, request.Value, shard)

	return &proto.CheckSetResponse{Status: found}, nil
}

func (database *KvServerState) CheckSet(key string, value string, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	entry := stripe.state[key].(Set)
	_, found := entry.value[value]

	return found
}

func (server *KvServerImpl) CheckSortedSet(
	ctx context.Context,
	request *proto.CheckSortedSetRequest,
) (*proto.CheckSortedSetResponse, error) {

	if request.Key == "" {
		return &proto.CheckSortedSetResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.CheckSortedSetResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	found := server.GetSortedSetDB().CheckSortedSet(request.Key, request.Value, shard)

	return &proto.CheckSortedSetResponse{Status: found}, nil
}

func (database *KvServerState) CheckSortedSet(key string, value string, shard int) bool {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return false
	}

	entry := stripe.state[key].(SortedSet)
	node := entry.value.GetByKey(value)

	return node != nil

}

func (server *KvServerImpl) PopList(
	ctx context.Context,
	request *proto.PopListRequest,
) (*proto.PopListResponse, error) {

	if request.Key == "" {
		return &proto.PopListResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.PopListResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	value, ok := server.GetListDB().PopList(request.Key, shard)

	if !ok {
		return &proto.PopListResponse{Status: ok}, status.Error(codes.Internal, "InternalError Failed to pop from list")
	}

	return &proto.PopListResponse{Status: ok, Value: value}, nil
}

func (database *KvServerState) PopList(key string, shard int) (string, bool) {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok || stripe.state[key].(List).value == nil {
		return "", false
	}

	entry := stripe.state[key].(List)

	if len(entry.value) == 0 {
		return "", false
	}

	value := entry.value[0]
	entry.value = entry.value[1:]
	stripe.state[key] = entry

	return value, true
}

func (server *KvServerImpl) GetRange(
	ctx context.Context,
	request *proto.GetRangeRequest,
) (*proto.GetRangeResponse, error) {

	if request.Key == "" {
		return &proto.GetRangeResponse{}, status.Error(codes.InvalidArgument, "Empty keys are not allowed")
	}

	if request.Start > request.End {
		return &proto.GetRangeResponse{}, status.Error(codes.InvalidArgument, "Start must be less than end")
	}

	server.rpcMutex.Lock()
	server.rpcMutex.Unlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())

	server.mySL.RLock()
	if !slices.Contains(server.myShards, shard) {
		server.mySL.RUnlock()
		return &proto.GetRangeResponse{}, status.Error(codes.NotFound, "Incorrect shard")
	}
	server.mySL.RUnlock()

	values, ok := server.GetSortedSetDB().GetRange(request.Key, request.Start, request.End, shard)

	if !ok {
		return &proto.GetRangeResponse{}, status.Error(codes.Internal, "InternalError Failed to get range")
	}

	return &proto.GetRangeResponse{Values: values}, nil
}

func (database *KvServerState) GetRange(key string, start int64, end int64, shard int) ([]string, bool) {
	stripe := database.stripes[shard]
	stripe.mutex.Lock()
	defer stripe.mutex.Unlock()

	if _, ok := stripe.state[key]; !ok {
		return nil, false
	}

	entry := stripe.state[key].(SortedSet)
	nodes := entry.value.GetByRankRange(int(start), int(end), false)

	values := make([]string, len(nodes))
	for i, node := range nodes {
		values[i] = node.Key()
	}

	return values, true
}

package kvtest

import (
	"testing"
	"time"

	"cs426.yale.edu/lab4/kv"
	"cs426.yale.edu/lab4/kv/proto"
	"github.com/stretchr/testify/assert"
)

// Like previous labs, you must write some tests on your own.
// Add your test cases in this file and submit them as extra_test.go.
// You must add at least 5 test cases, though you can add as many as you like.
//
// You can use any type of test already used in this lab: server
// tests, client tests, or integration tests.
//
// You can also write unit tests of any utility functions you have in utils.go
//
// Tests are run from an external package, so you are testing the public API
// only. You can make methods public (e.g. utils) by making them Capitalized.

// Tests that GetShardContents returns the correct data
func TestGetShardContentsSimple(t *testing.T) {
	setup := MakeTestSetup(
		kv.ShardMapState{
			NumShards: 1,
			Nodes:     makeNodeInfos(2),
			ShardsToNodes: map[int][]string{
				1: {"n1"},
			},
		},
	)

	// n1 hosts the shard, so we should be able to set data
	err := setup.NodeSet("n1", "abc", "123", 10*time.Second)
	assert.Nil(t, err)

	// GetShardContents should return the data we just set
	val, err := setup.nodes["n1"].GetShardContents(setup.ctx, &proto.GetShardContentsRequest{Shard: 1})
	assert.Nil(t, err)
	assert.Equal(t, "123", val.Values[0].Value)

	setup.Shutdown()
}

// Verifies that the shard function consisently returns the same shard for a given key
func TestConsistentHash(t *testing.T) {

	key := "testKey"
	numShards := 10
	shardId1 := kv.GetShardForKey(key, numShards)
	shardId2 := kv.GetShardForKey(key, numShards)
	shardId3 := kv.GetShardForKey(key, numShards+1)

	assert.Equal(t, shardId1, shardId2)
	assert.NotEqual(t, shardId1, shardId3)
}

// Verifies that the shard management works on 0 TTL values
func TestZeroTtl(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	_, wasFound, err := setup.NodeGet("n1", "gnd6")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	// Set a key with 0 TTL
	err = setup.NodeSet("n1", "gnd6", "gabe", 0*time.Second)
	assert.Nil(t, err)

	time.Sleep(100 * time.Millisecond)

	_, wasFound, err = setup.NodeGet("n1", "gnd6")
	assert.Nil(t, err)
	assert.False(t, wasFound)

}

// Verifies that repeated deletes don't destory system state
func TestRepeatedDelete(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	err := setup.NodeDelete("n1", "alice")
	assert.Nil(t, err)

	err = setup.NodeSet("n1", "alice", "ben", 10*time.Second)
	assert.Nil(t, err)

	val, wasFound, err := setup.NodeGet("n1", "alice")
	assert.True(t, wasFound)
	assert.Equal(t, "ben", val)
	assert.Nil(t, err)

	err = setup.NodeDelete("n1", "alice")
	assert.Nil(t, err)

	// Repeated delete of the same key should not cause an error
	err = setup.NodeDelete("n1", "alice")
	assert.Nil(t, err)

	setup.Shutdown()
}

// Verifies test expirations are set properly
func TestProperExpire(t *testing.T) {
	setup := MakeTestSetup(MakeMultiShardSingleNode())

	err := setup.NodeSet("n1", "alice", "ben", 10*time.Millisecond)
	assert.Nil(t, err)

	err = setup.NodeSet("n1", "gabe", "ben", 10*time.Millisecond)
	assert.Nil(t, err)

	val, wasFound, err := setup.NodeGet("n1", "alice")
	assert.True(t, wasFound)
	assert.Equal(t, "ben", val)
	assert.Nil(t, err)

	time.Sleep(12 * time.Millisecond)

	_, wasFound, err = setup.NodeGet("n1", "alice")
	assert.False(t, wasFound)
	assert.Nil(t, err)

	_, wasFound, err = setup.NodeGet("n1", "gabe")
	assert.False(t, wasFound)
	assert.Nil(t, err)

	setup.Shutdown()
}

func TestIntegrationExpire(t *testing.T) {

	// test that relies on client and server to match expiry time

	setup := MakeTestSetup(MakeBasicOneShard())

	err := setup.Set("abc", "123", 5*time.Millisecond)
	assert.Nil(t, err)

	val, wasFound, err := setup.Get("abc")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Equal(t, "123", val)

	time.Sleep(10 * time.Millisecond)
	_, wasFound, err = setup.Get("abc")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	setup.Shutdown()
}

// Check that a key timeout is handled properly from the client-side
func TestClientTimeout(t *testing.T) {
	// Similar to TestClientGetSingleNode: one node, one shard,
	// testing that Set/Delete RPCs are sent.
	setup := MakeTestSetup(MakeManyNodesWithManyShards(1000, 700))

	err := setup.Set("ben", "val", 1000*time.Millisecond)
	assert.Nil(t, err)
	// Sleep for 1 second to ensure that the key expires
	time.Sleep(1 * time.Second)
	_, success, err := setup.Get("alice")
	assert.False(t, success)

	// Set a new key
	err1 := setup.Set("gabe", "val", 1000*time.Millisecond)
	_, success, err = setup.Get("gabe")
	err2 := setup.Set("yang", "val", 1000*time.Millisecond)
	assert.Nil(t, err1)
	assert.Nil(t, err2)

	// Get one key
	assert.True(t, success)

	// Wait til the key expires
	time.Sleep(1 * time.Second)
	_, success, err = setup.Get("gabe")
	assert.False(t, success)

	_, success, err = setup.Get("yang")
	assert.False(t, success)

	setup.Shutdown()

}

// MultiSet Unit Test (same as TestBasic but replace Get with MultiSet)
func RunBasicMultiSet(t *testing.T, setup *TestSetup) {
	// For a given setup (nodes and shard placement), runs
	// very basic tests -- just get, set, and delete on one key.
	_, wasFound, err := setup.NodeGet("n1", "abc")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	err = setup.NodeMultiSet("n1", "abc", "123", 5*time.Second)
	assert.Nil(t, err)

	val, wasFound, err := setup.NodeGet("n1", "abc")
	assert.True(t, wasFound)
	assert.Equal(t, "123", val)
	assert.Nil(t, err)
	val, wasFound, err = setup.NodeGet("n1", "abc")
	assert.True(t, wasFound)
	assert.Equal(t, "123", val)
	assert.Nil(t, err)

	err = setup.NodeDelete("n1", "abc")
	assert.Nil(t, err)

	_, wasFound, err = setup.NodeGet("n1", "abc")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

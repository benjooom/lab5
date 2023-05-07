package kvtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.NodeCreateList("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the list
	err = setup.NodeAppendList("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Check value in list
	wasFound, err := setup.NodeCheckList("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove value from list
	err = setup.NodeRemoveList("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Check value not in list
	wasFound, err = setup.NodeCheckList("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)

}

func TestSetBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a set
	err := setup.NodeCreateSet("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the set
	err = setup.NodeAppendSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Check value in set
	wasFound, err := setup.NodeCheckSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove value from set
	err = setup.NodeRemoveSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Check value not in set
	wasFound, err = setup.NodeCheckSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)

}

func TestSortedSetBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a sorted set
	err := setup.NodeCreateSortedSet("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the sorted set
	err = setup.NodeAppendSortedSet("n1", "gnd6", "gabe", 50)
	assert.Nil(t, err)

	// Check value in sorted set
	wasFound, err := setup.NodeCheckSortedSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove value from sorted set
	err = setup.NodeRemoveSortedSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Check value not in sorted set
	wasFound, err = setup.NodeCheckSortedSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func TestSetRepeated(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a set
	err := setup.NodeCreateSet("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the set
	err = setup.NodeAppendSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Add value a second time (should be ignored)
	err = setup.NodeAppendSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Check value in set
	wasFound, err := setup.NodeCheckSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove value from set
	err = setup.NodeRemoveSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Check value not in set
	wasFound, err = setup.NodeCheckSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)

}

func TestSortedSetRepeated(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a sorted set
	err := setup.NodeCreateSortedSet("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the sorted set
	err = setup.NodeAppendSortedSet("n1", "gnd6", "gabe", 50)
	assert.Nil(t, err)

	// Add value a second time (should be ignored)
	err = setup.NodeAppendSortedSet("n1", "gnd6", "gabe", 50)
	assert.Nil(t, err)

	// Check value in sorted set
	wasFound, err := setup.NodeCheckSortedSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove value from sorted set
	err = setup.NodeRemoveSortedSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Check value not in sorted set
	wasFound, err = setup.NodeCheckSortedSet("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func TestListPopBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.NodeCreateList("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the list
	err = setup.NodeAppendList("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Pop value from list
	value, status, err := setup.NodePopList("n1", "gnd6")
	assert.Nil(t, err)
	assert.True(t, status)
	assert.Equal(t, "gabe", value)

	// Check value not in list
	wasFound, err := setup.NodeCheckList("n1", "gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func TestListPopLast(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.NodeCreateList("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a values to the list
	err = setup.NodeAppendList("n1", "gnd6", "alice")
	assert.Nil(t, err)
	err = setup.NodeAppendList("n1", "gnd6", "ben")
	assert.Nil(t, err)
	err = setup.NodeAppendList("n1", "gnd6", "joe")
	assert.Nil(t, err)
	err = setup.NodeAppendList("n1", "gnd6", "gabe")
	assert.Nil(t, err)

	// Pop value from list (should be the first inserted value)
	value, status, err := setup.NodePopList("n1", "gnd6")
	assert.Nil(t, err)
	assert.True(t, status)
	assert.Equal(t, "alice", value)

	// Check value not in list
	wasFound, err := setup.NodeCheckList("n1", "gnd6", "alice")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	// Ensure the pop in sequential order (queue)
	value, status, err = setup.NodePopList("n1", "gnd6")
	assert.Nil(t, err)
	assert.True(t, status)
	assert.Equal(t, "ben", value)

	value, status, err = setup.NodePopList("n1", "gnd6")
	assert.Nil(t, err)
	assert.True(t, status)
	assert.Equal(t, "joe", value)

	value, status, err = setup.NodePopList("n1", "gnd6")
	assert.Nil(t, err)
	assert.True(t, status)
	assert.Equal(t, "gabe", value)

	wasFound, err = setup.NodeCheckList("n1", "gnd6", "joe")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func TestListPopEmpty(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.NodeCreateList("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Pop value from empty list
	value, status, err := setup.NodePopList("n1", "gnd6")
	assert.Nil(t, err)
	assert.False(t, status)
	assert.Equal(t, "", value)
}

func TestListPopNonExistent(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Pop value from non-existent list
	value, status, err := setup.NodePopList("n1", "gnd6")
	assert.Error(t, err)
	assert.False(t, status)
	assert.Equal(t, "", value)
}

func TestGetRangeBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.NodeCreateSortedSet("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a values to the list
	err = setup.NodeAppendSortedSet("n1", "gnd6", "alice", 2104)
	assert.Nil(t, err)
	err = setup.NodeAppendSortedSet("n1", "gnd6", "ben", 31294)
	assert.Nil(t, err)
	err = setup.NodeAppendSortedSet("n1", "gnd6", "joe", 44)
	assert.Nil(t, err)
	err = setup.NodeAppendSortedSet("n1", "gnd6", "gabe", 512)
	assert.Nil(t, err)

	// Get range of values from list
	// 2, -1 --> 2nd value to 4th value
	values, err := setup.NodeGetRange("n1", "gnd6", 2, 4)
	assert.Nil(t, err)
	assert.Contains(t, values, "alice")
	assert.Contains(t, values, "ben")
	assert.Contains(t, values, "gabe")
	assert.NotContains(t, values, "joe")

	// Should return the list in rank order
	assert.Equal(t, []string{"gabe", "alice", "ben"}, values)

}

func TestGetRangeComplex(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.NodeCreateSortedSet("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a values to the list
	err = setup.NodeAppendSortedSet("n1", "gnd6", "alice", 1)
	assert.Nil(t, err)
	err = setup.NodeAppendSortedSet("n1", "gnd6", "ben", 2)
	assert.Nil(t, err)
	err = setup.NodeAppendSortedSet("n1", "gnd6", "joe", 3)
	assert.Nil(t, err)
	err = setup.NodeAppendSortedSet("n1", "gnd6", "gabe", 4)
	assert.Nil(t, err)

	// Get range of values from list
	values, err := setup.NodeGetRange("n1", "gnd6", 2, 2)
	assert.Nil(t, err)
	assert.Equal(t, []string{"ben"}, values)

	// Remove ben
	err = setup.NodeRemoveSortedSet("n1", "gnd6", "ben")
	assert.Nil(t, err)

	// Joe becomes new 2nd rank
	values, err = setup.NodeGetRange("n1", "gnd6", 2, 2)
	assert.Nil(t, err)
	assert.Equal(t, []string{"joe"}, values)

	// Should return entire list
	values, err = setup.NodeGetRange("n1", "gnd6", 1, -1)
	assert.Nil(t, err)
	assert.Equal(t, []string{"alice", "joe", "gabe"}, values)

}

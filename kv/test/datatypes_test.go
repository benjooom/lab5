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

func TestSortedSetGetRangeBasic(t *testing.T) {
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

func TestSortedSetGetRangeComplex(t *testing.T) {
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

func TestSortedSetGetRangeEmpty(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.NodeCreateSortedSet("n1", "gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Get range of values from empty list
	values, err := setup.NodeGetRange("n1", "gnd6", 1, -1)
	assert.Nil(t, err)
	assert.Equal(t, []string{}, values)
}

func TestGetRangeNonExistent(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Get range of values from non-existent list
	_, err := setup.NodeGetRange("n1", "gnd6", 1, -1)
	assert.Error(t, err)

}

func TestListTtl(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.NodeCreateList("n1", "gnd6", 1*time.Second)
	assert.Nil(t, err)

	// Add a value to the list
	err = setup.NodeAppendList("n1", "gnd6", "alice")
	assert.Nil(t, err)

	wasFound, err := setup.NodeCheckList("n1", "gnd6", "alice")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// sleep for 1 second (past TTL)
	time.Sleep(2 * time.Second)

	// Check that the list is gone
	wasFound, err = setup.NodeCheckList("n1", "gnd6", "alice")
	assert.Error(t, err)
	assert.False(t, wasFound)

	err = setup.NodeAppendList("n1", "gnd6", "alice")
	assert.Error(t, err)

	err = setup.NodeRemoveList("n1", "gnd6", "alice")
	assert.Error(t, err)

}

func TestSetTtl(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a set
	err := setup.NodeCreateSet("n1", "gnd6", 1*time.Second)
	assert.Nil(t, err)

	// Add a value to the set
	err = setup.NodeAppendSet("n1", "gnd6", "alice")
	assert.Nil(t, err)

	wasFound, err := setup.NodeCheckSet("n1", "gnd6", "alice")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// sleep for 1 second (past TTL)
	time.Sleep(2 * time.Second)

	// Check that the set is gone
	wasFound, err = setup.NodeCheckSet("n1", "gnd6", "alice")
	assert.Error(t, err)
	assert.False(t, wasFound)

	err = setup.NodeAppendSet("n1", "gnd6", "alice")
	assert.Error(t, err)

	err = setup.NodeRemoveSet("n1", "gnd6", "alice")
	assert.Error(t, err)

}

func TestSortedSetTtl(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a sorted set
	err := setup.NodeCreateSortedSet("n1", "gnd6", 1*time.Second)
	assert.Nil(t, err)

	// Add a value to the sorted set
	err = setup.NodeAppendSortedSet("n1", "gnd6", "alice", 1)
	assert.Nil(t, err)

	wasFound, err := setup.NodeCheckSortedSet("n1", "gnd6", "alice")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// sleep for 1 second (past TTL)
	time.Sleep(2 * time.Second)

	// Check that the sorted set is gone
	wasFound, err = setup.NodeCheckSortedSet("n1", "gnd6", "alice")
	assert.Error(t, err)
	assert.False(t, wasFound)

	err = setup.NodeAppendSortedSet("n1", "gnd6", "alice", 1)
	assert.Error(t, err)

	err = setup.NodeRemoveSortedSet("n1", "gnd6", "alice")
	assert.Error(t, err)

}

// Test that we can set an entire list and then get it back
func TestListGetSet(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Set the list
	err := setup.SetList("gnd6", []string{"alice", "bob", "charlie"}, 5*time.Second)
	assert.Nil(t, err)

	// Get the list
	values, err := setup.GetList("gnd6")
	assert.Nil(t, err)
	assert.Equal(t, []string{"alice", "bob", "charlie"}, values)
}

// Test that we can set an entire set and then get it back
func TestSetSet(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Set the set
	err := setup.SetSet("gnd6", []string{"alice", "bob", "charlie", "charlie"}, 5*time.Second)
	assert.Nil(t, err)

	// Get the set
	values, err := setup.GetSet("gnd6")
	assert.Nil(t, err)

	// NOTE: Can not use an Equal bc Set does not gaurantee order
	assert.Contains(t, values, "alice")
	assert.Contains(t, values, "bob")
	assert.Contains(t, values, "charlie")

}

// Test that we can pop off the first element of a list
func TestListGetSetWithPop(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Set the list
	err := setup.SetList("gnd6", []string{"alice", "bob", "charlie"}, 5*time.Second)
	assert.Nil(t, err)

	// Pop the first element
	value, err := setup.PopList("gnd6")
	assert.Nil(t, err)
	assert.Equal(t, "alice", value)

	// Get the list
	values, err := setup.GetList("gnd6")
	assert.Nil(t, err)
	assert.Equal(t, []string{"bob", "charlie"}, values)
}

func TestListIntegrationBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.CreateList("gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the list
	err = setup.AppendList("gnd6", "gabe")
	assert.Nil(t, err)

	// Check value in list
	wasFound, err := setup.CheckList("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove value from list
	removed, err := setup.RemoveList("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, removed)

	// Check value not in list
	wasFound, err = setup.CheckList("gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func TestListIntegrationMultipleAppend(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.CreateList("gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the list
	err = setup.AppendList("gnd6", "gabe")
	assert.Nil(t, err)

	// Add a duplicate value to the list
	err = setup.AppendList("gnd6", "gabe")
	assert.Nil(t, err)

	// Check value in list
	wasFound, err := setup.CheckList("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove 1 value from list
	removed, err := setup.RemoveList("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, removed)

	// Check other value still in list
	wasFound, err = setup.CheckList("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)
}

func TestSetIntegrationBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a set
	err := setup.CreateSet("gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the set
	err = setup.AppendSet("gnd6", "gabe")
	assert.Nil(t, err)

	// Check value in set
	wasFound, err := setup.CheckSet("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove value from set
	removed, err := setup.RemoveSet("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, removed)

	// Check value not in set
	wasFound, err = setup.CheckSet("gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func TestSortedSetIntegrationBasic(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a sorted set
	err := setup.CreateSortedSet("gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a value to the sorted set
	err = setup.AppendSortedSet("gnd6", "gabe", 50)
	assert.Nil(t, err)

	// Add value a second time (should be ignored)
	err = setup.AppendSortedSet("gnd6", "gabe", 50)
	assert.Nil(t, err)

	// Check value in sorted set
	wasFound, err := setup.CheckSortedSet("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, wasFound)

	// Remove value from sorted set
	removed, err := setup.RemoveSortedSet("gnd6", "gabe")
	assert.Nil(t, err)
	assert.True(t, removed)

	// Check value not in sorted set
	wasFound, err = setup.CheckSortedSet("gnd6", "gabe")
	assert.Nil(t, err)
	assert.False(t, wasFound)
}

func TestSortedSetIntegrationComplex(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	// Create a list
	err := setup.CreateSortedSet("gnd6", 5*time.Second)
	assert.Nil(t, err)

	// Add a values to the list
	err = setup.AppendSortedSet("gnd6", "alice", 1)
	assert.Nil(t, err)
	err = setup.AppendSortedSet("gnd6", "ben", 2)
	assert.Nil(t, err)
	err = setup.AppendSortedSet("gnd6", "joe", 3)
	assert.Nil(t, err)
	err = setup.AppendSortedSet("gnd6", "gabe", 4)
	assert.Nil(t, err)

	// Get range of values from list
	values, err := setup.GetRange("gnd6", 2, 2)
	assert.Nil(t, err)
	assert.Equal(t, []string{"ben"}, values)

	// Remove ben
	removed, err := setup.RemoveSortedSet("gnd6", "ben")
	assert.Nil(t, err)
	assert.True(t, removed)

	// Joe becomes new 2nd rank
	values, err = setup.GetRange("gnd6", 2, 2)
	assert.Nil(t, err)
	assert.Equal(t, []string{"joe"}, values)

	// Should return entire list
	values, err = setup.GetRange("gnd6", 1, -1)
	assert.Nil(t, err)
	assert.Equal(t, []string{"alice", "joe", "gabe"}, values)

}

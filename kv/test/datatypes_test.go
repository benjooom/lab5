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

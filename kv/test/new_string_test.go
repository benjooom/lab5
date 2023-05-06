package kvtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCAS(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	_, wasFound, err := setup.NodeGet("n1", "abc")
	assert.Nil(t, err)
	assert.False(t, wasFound)

	err = setup.NodeSet("n1", "abc", "123", 5*time.Second)
	assert.Nil(t, err)

	status, err := setup.NodeCAS("n1", "abc", "456", "123", 5*time.Second)
	assert.Nil(t, err)
	assert.True(t, status)

	value, wasFound, err := setup.NodeGet("n1", "abc")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Equal(t, "456", value)

}

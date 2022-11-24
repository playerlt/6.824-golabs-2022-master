package mr

import (
	"testing"
)

type A struct {
	issuedMapTasks map[interface{}]bool
}

func TestMap(t *testing.T) {
	a := A{}
	a.issuedMapTasks["a"] = true
	t.Logf("map: %v", a.issuedMapTasks)
}

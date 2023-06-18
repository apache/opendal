package opendal

import "testing"

func TestNew(t *testing.T) {
	op := NewOperator("memory")
	t.Logf("%v", op)
}

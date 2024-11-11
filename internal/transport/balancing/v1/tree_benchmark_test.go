package v1

import (
	"github.com/stretchr/testify/require"

	"testing"
)

func BenchmarkTree_GetConn(b *testing.B) {
	const (
		lvl      = 3
		children = 3
	)
	tree, err := createTree(b, lvl)
	require.NoError(b, err)

	fillTree(b, tree, func(pID uint64) *conn {
		return &conn{
			alive: true,
		}
	}, lvl, children)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tree.GetConn()
	}
}

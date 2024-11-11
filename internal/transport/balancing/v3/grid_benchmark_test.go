package v3

import "testing"

func BenchmarkGetConn(b *testing.B) {
	const (
		children = 3
	)
	balancer := NewGrid[*conn, conn](Config{
		ConnsPerEndpoint: children,
		IgnoreLocations:  true, // all connections will be in default location
	})

	fillTree(b, balancer, func(pID uint64) *conn {
		c := &conn{
			alive: true,
			id:    pID,
		}

		return c
	}, children)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = balancer.GetConn()
	}
}

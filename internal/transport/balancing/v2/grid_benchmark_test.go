package v2

import (
	"context"
	"sync"
	"testing"
)

func BenchmarkGrid_GetBalanced(b *testing.B) {
	const (
		lvl      = 3
		children = 3
	)
	gr := createGrid(b, lvl, children)

	ctx, cancel := context.WithCancel(context.Background())
	wgg := &sync.WaitGroup{}
	wgg.Add(1)
	go gr.Run(ctx, wgg)

	fillGrid(b, gr, func(pID uint64) *conn {
		return &conn{
			alive: true,
			uid:   pID,
		}
	}, lvl, children)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = gr.GetBalanced()
	}
	b.StopTimer()
	cancel()
	wgg.Wait()
}

package v2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type conn struct {
	uid   uint64
	alive bool
}

func (c *conn) Alive() bool {
	return c.alive
}

func (c *conn) Close() error {
	return nil
}

func TestAddDel(t *testing.T) {
	type args struct {
		lvl     int
		addPath []string
		delPath []string
		connNum int
		alive   bool
	}
	tests := []struct {
		name        string
		args        args
		addErr      error
		delErr      error
		nilAfterAdd bool
		nilAfterDel bool
	}{
		{
			name: "alive",
			args: args{
				lvl:     3,
				addPath: []string{"1", "2"},
				delPath: []string{"1", "2"},
				connNum: 1,
				alive:   true,
			},
			nilAfterAdd: false,
			nilAfterDel: true,
		},
		{
			name: "not alive",
			args: args{
				lvl:     3,
				addPath: []string{"1", "2"},
				delPath: []string{"1", "2"},
				connNum: 10,
				alive:   false,
			},
			nilAfterAdd: true,
			nilAfterDel: true,
		},
		{
			name: "del wrong path",
			args: args{
				lvl:     3,
				addPath: []string{"1", "2"},
				delPath: []string{"3", "4"},
				connNum: 10,
				alive:   true,
			},
			nilAfterAdd: false,
			nilAfterDel: false,
			delErr:      ErrPathDoesNotExist,
		},
		{
			name: "del wrong path 2",
			args: args{
				lvl:     3,
				addPath: []string{"1", "2"},
				delPath: []string{"1", "3"},
				connNum: 10,
				alive:   true,
			},
			nilAfterAdd: false,
			nilAfterDel: false,
			delErr:      ErrPathDoesNotExist,
		},
		{
			name: "del incomplete path",
			args: args{
				lvl:     3,
				addPath: []string{"1", "2"},
				delPath: []string{"1"},
				connNum: 10,
				alive:   true,
			},
			nilAfterAdd: false,
			nilAfterDel: false,
			delErr:      ErrPathLen,
		},
		{
			name: "del too long path",
			args: args{
				lvl:     3,
				addPath: []string{"1", "2"},
				delPath: []string{"1", "2", "3"},
				connNum: 10,
				alive:   true,
			},
			nilAfterAdd: false,
			nilAfterDel: false,
			delErr:      ErrPathLen,
		},
		{
			name: "add incomplete path",
			args: args{
				lvl:     3,
				addPath: []string{"1"},
				delPath: []string{"1", "2"},
				connNum: 10,
				alive:   true,
			},
			nilAfterAdd: true,
			nilAfterDel: true,
			addErr:      ErrPathLen,
			delErr:      ErrPathDoesNotExist,
		},
		{
			name: "add too long path",
			args: args{
				lvl:     3,
				addPath: []string{"1", "2", "3"},
				delPath: []string{"1", "2"},
				connNum: 10,
				alive:   true,
			},
			nilAfterAdd: true,
			nilAfterDel: true,
			addErr:      ErrPathLen,
			delErr:      ErrPathDoesNotExist,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := createTree(t, tt.args.lvl)
			require.NoError(t, err)

			err = tr.addPath(path[*conn, conn]{
				ids: tt.args.addPath,

				connectionConfig: connectionConfig[*conn, conn]{
					ConnFunc: func() (*conn, error) {
						return &conn{alive: tt.args.alive}, nil
					},
					ConnNumber: tt.args.connNum,
				},
			})
			require.ErrorIs(t, err, tt.addErr)

			got := tr.node.getBalanced()
			require.Equal(t, tt.nilAfterAdd, got == nil)

			err = tr.deletePath(path[*conn, conn]{ids: tt.args.delPath})
			require.ErrorIs(t, err, tt.delErr)

			got = tr.node.getBalanced()
			require.Equal(t, tt.nilAfterDel, got == nil)
		})
	}
}

func createTree(t *testing.T, lvl int) (*tree[*conn, conn], error) {
	t.Helper()

	levels := make([]Level, lvl)
	for i := 1; i < lvl; i++ {
		levels[i-1] = Level{
			Kind:   fmt.Sprintf("Level%d", i),
			Policy: PolicyKindRoundRobin,
		}
	}
	levels[lvl-1] = Level{
		Kind:   levelKindConnection,
		Policy: PolicyKindRoundRobin,
	}

	return newTree[*conn, conn](treeConfig[*conn, conn]{
		levels: levels,
	})
}

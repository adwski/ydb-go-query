package endpoints

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
)

func TestDB(t *testing.T) {
	type args struct {
		epsMap Map
		epsNew []*Ydb_Discovery.EndpointInfo
	}
	type want struct {
		ann  Announce
		len  int
		prev int
		cmp  bool
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "add one endpoint",
			args: args{
				epsNew: []*Ydb_Discovery.EndpointInfo{
					{
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeId:   1234,
					},
				},
			},
			want: want{
				len: 1,
				ann: Announce{
					Add: Map{
						InfoShort{
							Address:  "1.1.1.1",
							Port:     1234,
							Location: "0",
							NodeID:   1234,
						}: {
							Address:  "1.1.1.1",
							Port:     1234,
							Location: "0",
							NodeId:   1234,
						},
					},
					Del: []InfoShort{},
				},
			},
		},
		{
			name: "add two endpoints",
			args: args{
				epsNew: []*Ydb_Discovery.EndpointInfo{
					{
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeId:   1234,
					},
					{
						Address:  "2.2.2.2",
						Port:     1234,
						Location: "1",
						NodeId:   1234,
					},
				},
			},
			want: want{
				ann: Announce{
					Add: Map{
						{
							Address:  "1.1.1.1",
							Port:     1234,
							Location: "0",
							NodeID:   1234,
						}: {
							Address:  "1.1.1.1",
							Port:     1234,
							Location: "0",
							NodeId:   1234,
						},
						{
							Address:  "2.2.2.2",
							Port:     1234,
							Location: "1",
							NodeID:   1234,
						}: {
							Address:  "2.2.2.2",
							Port:     1234,
							Location: "1",
							NodeId:   1234,
						},
					},
					Del: []InfoShort{},
				},
				len: 2,
			},
		},
		{
			name: "add existing endpoint",
			args: args{
				epsMap: map[InfoShort]*Ydb_Discovery.EndpointInfo{
					{
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeID:   1234,
					}: {
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeId:   1234,
					},
				},
				epsNew: []*Ydb_Discovery.EndpointInfo{
					{
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeId:   1234,
					},
				},
			},
			want: want{
				cmp:  true,
				prev: 1,
				len:  1,
				ann: Announce{
					Add: map[InfoShort]*Ydb_Discovery.EndpointInfo{},
					Del: []InfoShort{},
				},
			},
		},
		{
			name: "add one more",
			args: args{
				epsMap: map[InfoShort]*Ydb_Discovery.EndpointInfo{
					{
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeID:   1234,
					}: {
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeId:   1234,
					},
				},
				epsNew: []*Ydb_Discovery.EndpointInfo{
					{
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeId:   1234,
					},
					{
						Address:  "2.2.2.2",
						Port:     1234,
						Location: "1",
						NodeId:   1234,
					},
				},
			},
			want: want{
				cmp:  false,
				prev: 1,
				len:  2,
				ann: Announce{
					Add: map[InfoShort]*Ydb_Discovery.EndpointInfo{
						{
							Address:  "2.2.2.2",
							Port:     1234,
							Location: "1",
							NodeID:   1234,
						}: {
							Address:  "2.2.2.2",
							Port:     1234,
							Location: "1",
							NodeId:   1234,
						},
					},
					Del: []InfoShort{},
				},
			},
		},
		{
			name: "add and del",
			args: args{
				epsMap: map[InfoShort]*Ydb_Discovery.EndpointInfo{
					{
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeID:   1234,
					}: {
						Address:  "1.1.1.1",
						Port:     1234,
						Location: "0",
						NodeId:   1234,
					},
				},
				epsNew: []*Ydb_Discovery.EndpointInfo{
					{
						Address:  "2.2.2.2",
						Port:     1234,
						Location: "1",
						NodeId:   1234,
					},
				},
			},
			want: want{
				cmp:  false,
				prev: 1,
				len:  1,
				ann: Announce{
					Add: map[InfoShort]*Ydb_Discovery.EndpointInfo{
						{
							Address:  "2.2.2.2",
							Port:     1234,
							Location: "1",
							NodeID:   1234,
						}: {
							Address:  "2.2.2.2",
							Port:     1234,
							Location: "1",
							NodeId:   1234,
						},
					},
					Del: []InfoShort{
						{
							Address:  "1.1.1.1",
							Port:     1234,
							Location: "0",
							NodeID:   1234,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := NewDB()
			db.dbm = tt.args.epsMap

			assert.Equal(t, tt.want.cmp, db.Compare(tt.args.epsNew))

			ann, prev, ln := db.Update(tt.args.epsNew)

			assert.Equal(t, tt.want.prev, prev)
			assert.Equal(t, tt.want.len, ln)
			assert.Equal(t, tt.want.ann, ann)
		})
	}
}

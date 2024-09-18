package endpoints

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
)

func TestFilter_Filter(t *testing.T) {
	type args struct {
		filterFunc func() *Filter
		endpoints  []*Ydb_Discovery.EndpointInfo
	}
	type want struct {
		preferred []*Ydb_Discovery.EndpointInfo
		required  []*Ydb_Discovery.EndpointInfo
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "required and preferred",
			args: args{
				filterFunc: func() *Filter {
					f := NewFilter()
					f.Require = &Require{
						Services:  []string{"aaa", "bbb"},
						Locations: []string{"xxx", "yyy", "zzz"},
					}
					f.Prefer = &Prefer{Locations: []string{"yyy"}}

					return f
				},
				endpoints: []*Ydb_Discovery.EndpointInfo{
					{
						Service:  []string{"aaa", "bbb", "ccc"},
						Location: "xxx",
					},
					{
						Service:  []string{"aaa", "bbb"},
						Location: "yyy",
					},
					{
						Service:  []string{"aaa", "ccc"},
						Location: "yyy",
					},
					{
						Service:  []string{"aaa", "qwe"},
						Location: "zzz",
					},
				},
			},
			want: want{
				preferred: []*Ydb_Discovery.EndpointInfo{
					{
						Service:  []string{"aaa", "bbb"},
						Location: "yyy",
					},
				},
				required: []*Ydb_Discovery.EndpointInfo{
					{
						Service:  []string{"aaa", "bbb", "ccc"},
						Location: "xxx",
					},
				},
			},
		},
		{
			name: "with query service",
			args: args{
				filterFunc: func() *Filter {
					return NewFilter().WithQueryService()
				},
				endpoints: []*Ydb_Discovery.EndpointInfo{
					{
						Service:  []string{"aaa", "bbb", "query_service"},
						Location: "xxx",
					},
					{
						Service:  []string{"aaa", "bbb"},
						Location: "yyy",
					},
				},
			},
			want: want{
				preferred: []*Ydb_Discovery.EndpointInfo{
					{
						Service:  []string{"aaa", "bbb", "query_service"},
						Location: "xxx",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := tt.args.filterFunc()
			pref, req := filter.Filter(tt.args.endpoints)

			assert.Equal(t, tt.want.preferred, pref)
			assert.Equal(t, tt.want.required, req)
		})
	}
}

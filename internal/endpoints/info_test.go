package endpoints

import "testing"

func TestInfoShort(t *testing.T) {
	type fields struct {
		Address string
		Port    uint32
	}
	tests := []struct {
		name        string
		fields      fields
		wantAddress string
		wantPort    uint32
	}{
		{
			name: "ipv4",
			fields: fields{
				Address: "127.0.0.1",
				Port:    1234,
			},
			wantAddress: "127.0.0.1",
			wantPort:    1234,
		},
		{
			name: "ipv6",
			fields: fields{
				Address: "f00a::1",
				Port:    1000,
			},
			wantAddress: "f00a::1",
			wantPort:    1000,
		},
		{
			name: "hostname",
			fields: fields{
				Address: "example.com",
				Port:    9999,
			},
			wantAddress: "example.com",
			wantPort:    9999,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eis := &InfoShort{
				Address: tt.fields.Address,
				Port:    tt.fields.Port,
			}
			if got := eis.GetAddress(); got != tt.wantAddress {
				t.Errorf("GetAddress() = %v, want %v", got, tt.wantAddress)
			}
			if got := eis.GetPort(); got != tt.wantPort {
				t.Errorf("GetAddress() = %v, want %v", got, tt.wantPort)
			}
		})
	}
}

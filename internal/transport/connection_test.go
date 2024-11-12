package transport

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConnection_ID(t *testing.T) {
	endpointID := uint64(123123)
	conn := &Connection{
		endpointID: endpointID,
	}

	assert.Equal(t, conn.endpointID, conn.ID())
}

func TestConnection_Close(t *testing.T) {
	cc, err := grpc.Dial("localhost:1234", grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	conn := &Connection{
		ClientConn: cc,
	}

	require.NoError(t, conn.Close())
	assert.Equal(t, connectivity.Shutdown, cc.GetState())
}

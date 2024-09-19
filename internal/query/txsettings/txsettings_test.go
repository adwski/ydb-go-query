package txsettings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

func TestOnlineReadOnly(t *testing.T) {
	assert.Equal(t, &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: &Ydb_Query.OnlineModeSettings{},
		},
	}, OnlineReadOnly())
}

func TestOnlineReadOnlyInconsistent(t *testing.T) {
	assert.Equal(t, &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: &Ydb_Query.OnlineModeSettings{
				AllowInconsistentReads: true,
			},
		},
	}, OnlineReadOnlyInconsistent())
}

func TestSnapshotReadOnly(t *testing.T) {
	assert.Equal(t, &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_SnapshotReadOnly{
			SnapshotReadOnly: &Ydb_Query.SnapshotModeSettings{},
		},
	}, SnapshotReadOnly())
}

func TestStaleReadOnly(t *testing.T) {
	assert.Equal(t, &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_StaleReadOnly{
			StaleReadOnly: &Ydb_Query.StaleModeSettings{},
		},
	}, StaleReadOnly())
}

func TestSerializableReadWrite(t *testing.T) {
	assert.Equal(t, &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
			SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
		},
	}, SerializableReadWrite())
}

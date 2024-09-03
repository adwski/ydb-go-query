package txsettings

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"

func OnlineReadOnly() *Ydb_Query.TransactionSettings {
	return &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: &Ydb_Query.OnlineModeSettings{},
		},
	}
}

func OnlineReadOnlyInconsistent() *Ydb_Query.TransactionSettings {
	return &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_OnlineReadOnly{
			OnlineReadOnly: &Ydb_Query.OnlineModeSettings{
				AllowInconsistentReads: true,
			},
		},
	}
}

func SnapshotReadOnly() *Ydb_Query.TransactionSettings {
	return &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_SnapshotReadOnly{
			SnapshotReadOnly: &Ydb_Query.SnapshotModeSettings{},
		},
	}
}

func StaleReadOnly() *Ydb_Query.TransactionSettings {
	return &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_StaleReadOnly{
			StaleReadOnly: &Ydb_Query.StaleModeSettings{},
		},
	}
}

func SerializableReadWrite() *Ydb_Query.TransactionSettings {
	return &Ydb_Query.TransactionSettings{
		TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
			SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
		},
	}
}

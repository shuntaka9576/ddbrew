package ddbrew

type DDBAction string

const (
	DDB_ACTION_PUT    DDBAction = "PUT"
	DDB_ACTION_DELETE DDBAction = "DELETE"
	DDB_ACTION_READ   DDBAction = "READ"
)

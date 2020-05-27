package cassandra

type cassGroup struct {
	ID   string `cql:"group_id"`
	Name string `cql:"group_name"`
}

type cassTreeGroup struct {
	TreeID  int64  `cql:"tree_id"`
	GroupID string `cql:"group_id"`
}

type cassTree struct {
	TreeID             int64  `cql:"tree_id"`
	CreateTimeMillis   int64  `cql:"create_time_millis"`
	UpdateTimeMillis   int64  `cql:"update_time_millis"`
	DeleteTimeMillis   int64  `cql:"delete_time_millis"`
	Deleted            bool   `cql:"deleted"`
	DisplayName        string `cql:"display_name"`
	Description        string `cql:"description"`
	TreeState          string `cql:"tree_state"`
	TreeType           string `cql:"tree_type"`
	HashStrategy       string `cql:"hash_strategy"`
	HashAlgorithm      string `cql:"hash_algorithm"`
	SignatureAlgorithm string `cql:"signature_algorithm"`
	// TODO(phad): Should be `cql:"max_root_duration_millis"`.  GetTree fails with
	// ```can not unmarshal duration into *int64``` - possibly down in gocql?
	MaxRootDurationMillis int64  `cql:"-"`
	PrivateKey            []byte `cql:"private_key"`
	PublicKey             []byte `cql:"public_key"`
}

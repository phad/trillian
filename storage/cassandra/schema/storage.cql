CREATE KEYSPACE test WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};

CREATE TABLE test.trees(
  tree_id                  bigint,
  tree_state               text,
  tree_type                text,
  hash_strategy            text,
  hash_algorithm           text,
  signature_algorithm      text,
  display_name             varchar,
  description              varchar,
  create_time_millis       timestamp,
  update_time_millis       timestamp,
  max_root_duration_millis duration,
  private_key              blob,
  public_key               blob,
  deleted                  boolean,
  delete_time_millis       bigint,
  current_slr_json	       blob,
  root_signature	         blob,
  PRIMARY KEY(tree_id)
);

CREATE TABLE test.groups(
  group_id   uuid,
  group_name text,
  PRIMARY KEY (group_id)
);

CREATE TABLE test.groups_by_name(
  group_name text,
  group_id   uuid,
  PRIMARY KEY ((group_name), group_id)
) WITH CLUSTERING ORDER BY (group_id ASC);

CREATE TABLE test.trees_by_group_id(
  group_id uuid,
  tree_id  bigint,
  PRIMARY KEY ((group_id), tree_id)
) WITH CLUSTERING ORDER BY (tree_id ASC);

CREATE TABLE test.tree_heads(
    tree_id         bigint,
    revision        bigint,
    timestamp_nanos bigint,
    size            bigint,
    root_hash       blob,
    root_signature  blob,
    PRIMARY KEY ((tree_id, revision), timestamp_nanos)
) WITH CLUSTERING ORDER BY (timestamp_nanos DESC);

CREATE TABLE test.leaf_data(
  tree_id               bigint,
  leaf_identity_hash    blob,
  leaf_value            blob,
  extra_data            blob,
  queue_timestamp_nanos bigint,
  PRIMARY KEY((tree_id, leaf_identity_hash))
);

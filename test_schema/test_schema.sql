DROP TABLE users;

DROP TABLE user_groups;

DROP TABLE user_group_associations;

DROP TABLE user_statuses;

DROP TABLE otherdb;

CREATE TABLE users (
    id TEXT PRIMARY KEY,
    create_timestamp TIMESTAMPTZ,
    update_id TEXT
);

CREATE INDEX users_update_id_idx ON users (update_id);

CREATE TABLE user_groups (
    id TEXT PRIMARY KEY,
    create_timestamp TIMESTAMPTZ
);

CREATE TABLE user_group_associations (
    id TEXT PRIMARY KEY,
    create_timestamp TIMESTAMPTZ
);

CREATE TABLE user_statuses (
    id TEXT PRIMARY KEY,
    user_id TEXT,
    create_timestamp TIMESTAMPTZ,
    status BIGINT
);

CREATE TABLE otherdb (id TEXT);

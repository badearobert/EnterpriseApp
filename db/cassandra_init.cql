CREATE KEYSPACE IF NOT EXISTS mykeyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE mykeyspace;

CREATE TABLE IF NOT EXISTS users (
    user_id text PRIMARY KEY,
    data text
);

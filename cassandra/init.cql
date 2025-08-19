CREATE KEYSPACE IF NOT EXISTS users_elt
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS users_elt.users (
    name TEXT,
    email TEXT,
    gender TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    location TEXT,
    phone TEXT,
    cell TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (email, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

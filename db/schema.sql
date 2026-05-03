-- ELA RPC Server Database Schema
-- Run as postgres superuser to create the user and database,
-- then run the table creation as ela_indexer.

-- Step 1: Create user and database (run as postgres)
-- CREATE USER ela_indexer WITH PASSWORD 'YOUR_STRONG_PASSWORD' CONNECTION LIMIT 20;
-- CREATE DATABASE ela_index OWNER ela_indexer;
-- REVOKE ALL ON DATABASE ela_index FROM PUBLIC;
-- GRANT CONNECT ON DATABASE ela_index TO ela_indexer;

-- Step 2: Set security limits (run as postgres)
-- ALTER USER ela_indexer SET statement_timeout = '10s';

-- Step 3: Create tables (run as ela_indexer on ela_index database)

CREATE TABLE IF NOT EXISTS tx_outputs (
    txid    CHAR(64)    NOT NULL,
    n       INT         NOT NULL,
    address VARCHAR(34) NOT NULL,
    value   TEXT        NOT NULL,
    PRIMARY KEY (txid, n)
);

CREATE TABLE IF NOT EXISTS address_transactions (
    address       VARCHAR(34) NOT NULL,
    txid          CHAR(64)    NOT NULL,
    height        BIGINT      NOT NULL,
    direction     VARCHAR(8)  NOT NULL,
    value         TEXT        NOT NULL,
    fee           TEXT        NOT NULL DEFAULT '0',
    timestamp     BIGINT      NOT NULL,
    tx_type       INT         NOT NULL DEFAULT 0,
    vote_category INT         NOT NULL DEFAULT 0,
    memo          TEXT        NOT NULL DEFAULT '',
    inputs        TEXT        NOT NULL DEFAULT '[]',
    outputs       TEXT        NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS address_tx_counts (
    address  VARCHAR(34) PRIMARY KEY,
    tx_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS cr_proposal_reviews (
    did              VARCHAR(64) NOT NULL,
    proposal_hash    CHAR(64)    NOT NULL,
    title            TEXT        NOT NULL DEFAULT '',
    proposal_state   VARCHAR(32) NOT NULL DEFAULT '',
    opinion          VARCHAR(16) NOT NULL,
    opinion_hash     CHAR(64)    NOT NULL DEFAULT '',
    opinion_message  TEXT        NOT NULL DEFAULT '',
    review_height    BIGINT      NOT NULL,
    review_timestamp BIGINT      NOT NULL,
    PRIMARY KEY (did, proposal_hash)
);

CREATE TABLE IF NOT EXISTS cr_proposals (
    proposal_hash CHAR(64) PRIMARY KEY,
    title         TEXT        NOT NULL DEFAULT '',
    state         VARCHAR(32) NOT NULL DEFAULT '',
    last_updated  BIGINT      NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sync_state (
    key   VARCHAR(32) PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO sync_state (key, value) VALUES ('last_height', '0') ON CONFLICT DO NOTHING;
INSERT INTO sync_state (key, value) VALUES ('last_hash', '') ON CONFLICT DO NOTHING;

-- Safety net: ensure ela_indexer owns all tables even if this script
-- was accidentally run as the postgres superuser.
DO $$
BEGIN
    EXECUTE 'ALTER TABLE IF EXISTS tx_outputs OWNER TO ela_indexer';
    EXECUTE 'ALTER TABLE IF EXISTS address_transactions OWNER TO ela_indexer';
    EXECUTE 'ALTER TABLE IF EXISTS address_tx_counts OWNER TO ela_indexer';
    EXECUTE 'ALTER TABLE IF EXISTS cr_proposal_reviews OWNER TO ela_indexer';
    EXECUTE 'ALTER TABLE IF EXISTS cr_proposals OWNER TO ela_indexer';
    EXECUTE 'ALTER TABLE IF EXISTS sync_state OWNER TO ela_indexer';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Could not change table ownership (run as superuser to fix): %', SQLERRM;
END;
$$;

-- Indexes (created after initial sync for performance, but defined here for reference)
-- CREATE INDEX idx_addrtx_addr_height ON address_transactions (address, height DESC);
-- CREATE INDEX idx_addrtx_height ON address_transactions (height);

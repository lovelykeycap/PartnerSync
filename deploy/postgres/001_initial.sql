BEGIN;

CREATE SCHEMA sync;
CREATE SCHEMA internal_system;
CREATE SCHEMA partner_demo;

CREATE TABLE sync.reconciliation (
    partner_id varchar(60) NOT NULL,
    order_id varchar(100) NOT NULL,
    state text NOT NULL CHECK (state IN ('consistent', 'pending', 'conflict')),
    source_version bigint NOT NULL CHECK (source_version > 0),
    target_revision bigint NOT NULL CHECK (target_revision >= 0),
    checked_at timestamptz NOT NULL,
    PRIMARY KEY (partner_id, order_id)
);
CREATE INDEX ix_reconciliation_checked ON sync.reconciliation (checked_at DESC);

CREATE TABLE sync.outbox (
    id uuid PRIMARY KEY,
    dedup_key varchar(64) NOT NULL UNIQUE,
    message_key text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error text
);
CREATE INDEX ix_outbox_pending ON sync.outbox (next_attempt_at, created_at, id)
    WHERE published_at IS NULL;

CREATE TABLE internal_system.orders (
    partner_id varchar(60) NOT NULL,
    order_id varchar(100) NOT NULL,
    status text NOT NULL CHECK (status IN ('created', 'paid', 'shipped', 'cancelled')),
    total_minor bigint NOT NULL CHECK (total_minor >= 0),
    currency varchar(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    source_version bigint NOT NULL CHECK (source_version > 0),
    revision bigint NOT NULL CHECK (revision > 0),
    PRIMARY KEY (partner_id, order_id)
);

CREATE TABLE internal_system.inbox (
    event_id uuid PRIMARY KEY,
    outcome integer NOT NULL CHECK (outcome IN (1, 2, 3)),
    request_hash bytea NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE partner_demo.orders (
    order_id varchar(100) PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('created', 'paid', 'shipped', 'cancelled')),
    total_minor bigint NOT NULL CHECK (total_minor >= 0),
    currency varchar(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    version bigint NOT NULL CHECK (version > 0)
);

INSERT INTO partner_demo.orders (order_id, status, total_minor, currency, version) VALUES
    ('A-1001', 'paid', 149900, 'RUB', 2),
    ('A-1002', 'shipped', 429000, 'RUB', 3),
    ('A-1003', 'created', 99000, 'RUB', 1);

-- A-1001 is stale, A-1002 is missing, A-1003 already matches the partner.
INSERT INTO internal_system.orders
    (partner_id, order_id, status, total_minor, currency, source_version, revision) VALUES
    ('acme', 'A-1001', 'created', 149900, 'RUB', 1, 1),
    ('acme', 'A-1003', 'created', 99000, 'RUB', 1, 1);

COMMIT;

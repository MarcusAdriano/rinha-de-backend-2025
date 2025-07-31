CREATE UNLOGGED TABLE payments (
    correlationId UUID PRIMARY KEY,
    amount DECIMAL NOT NULL,
    requested_at TIMESTAMP NOT NULL,
    api INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX payments_requested_at ON payments (requested_at);
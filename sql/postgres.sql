CREATE TABLE IF NOT EXISTS fiscal_errors (
    ts        TIMESTAMP DEFAULT now(),
    reason    TEXT NOT NULL,
    payload   JSONB NOT NULL
);
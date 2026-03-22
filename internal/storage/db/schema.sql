CREATE TABLE IF NOT EXISTS inverted_index (
    term TEXT,
    content_hash BIGINT,

    PRIMARY KEY (term, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_term ON inverted_index(term);

CREATE TABLE IF NOT EXISTS documents (
    content_hash BIGINT PRIMARY KEY,
    url TEXT,
    title TEXT,
    snippet TEXT,
    object_key TEXT,
    inbound_links INT
);
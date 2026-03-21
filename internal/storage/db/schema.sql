CREATE TABLE inverted_index (
    term TEXT,
    content_hash BIGINT,

    PRIMARY KEY (term, content_hash)
);

CREATE INDEX idx_term ON inverted_index(term);

CREATE TABLE documents (
    content_hash BIGINT PRIMARY KEY,
    url TEXT,
    title TEXT,
    snippet TEXT,
    object_key TEXT,
    inbound_links INT
);
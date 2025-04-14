CREATE KEYSPACE IF NOT EXISTS search_engine
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE search_engine;
CREATE TABLE IF NOT EXISTS documents (
    doc_id TEXT PRIMARY KEY,
    title TEXT,
    content TEXT,
    length INT
);
CREATE TABLE IF NOT EXISTS inverted_index (
    term_text TEXT,
    doc_id TEXT,
    tf INT,
    PRIMARY KEY (term_text, doc_id)
);
CREATE TABLE IF NOT EXISTS documents_stats (
    stat_name TEXT PRIMARY KEY,
    stat_value FLOAT
);

import sys
from cassandra.cluster import Cluster


def connect_cassandra():
    try:
        cluster = Cluster(['cassandra-server'], port=9042)
        session = cluster.connect('search_engine')
        return session
    except Exception as e:
        sys.stderr.write(f"Failed to connect to Cassandra: ERROR: {str(e)}\n")
        sys.exit(1)


def main():
    session = connect_cassandra()
    for line in sys.stdin:
        try:
            term, doc_id, tf = line.strip().split('\t')
            session.execute(
                "INSERT INTO inverted_index (term_text, doc_id, tf) VALUES (%s, %s, %s)",
                (term, doc_id, int(tf))
            )
        except Exception as e:
            sys.stderr.write(f"Error processing line: ERROR: {str(e)}\n")
            sys.exit(2)


if __name__ == "__main__":
    main()

import sys
from cassandra.cluster import Cluster

def connect_cassandra():
    try:
        cluster = Cluster(['cassandra-server'], port=9042)
        session = cluster.connect('search_engine')
        return session
    except Exception as e:
        sys.stderr.write(f"Failed to connect to Cassandra: {str(e)}\n")
        sys.exit(1)

def main():
    session = connect_cassandra()
    for line in sys.stdin:
        try:
            doc_id, title, text = line.strip().split('\t')
            terms = text.lower().split()
            term_counts = {}
            for term in terms:
                term_counts[term] = term_counts.get(term, 0) + 1
            doc_length = len(text.split())
            session.execute(
                "INSERT INTO documents (doc_id, title, content, length) VALUES (%s, %s, %s, %s)",
                (doc_id, title, text, doc_length)
            )
            for term, count in term_counts.items():
                print(f"{term}\t{doc_id}\t{count}")
        except Exception as e:
            sys.stderr.write(f"Error processing line: {str(e)}\n")


if __name__ == "__main__":
    main()

import sys
from cassandra.cluster import Cluster

def connect_cassandra():
    cluster = Cluster(['cassandra-server'])
    return cluster.connect('search_engine')

session = connect_cassandra()

total_docs = 0
total_length = 0

for line in sys.stdin:
    doc_id, length = line.strip().split('\t')
    total_docs += 1
    total_length += int(length)

avg_length = total_length / total_docs if total_docs > 0 else 0

session.execute(
    "INSERT INTO documents_stats (stat_name, stat_value) VALUES (%s, %s)",
    ('total_docs', total_docs)
)

session.execute(
    "INSERT INTO documents_stats (stat_name, stat_value) VALUES (%s, %s)",
    ('avg_length', avg_length)
)

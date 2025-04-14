import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions as F
import math


def main(query):
    conf = SparkConf() \
        .setAppName("BM25Ranker") \
        .set("spark.cassandra.connection.host", "cassandra-server")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    try:
        k1 = 1.2
        b = 0.75

        stats_df = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="documents_stats", keyspace="search_engine") \
            .load()

        total_docs = stats_df.filter("stat_name = 'total_docs'").first().stat_value
        avg_length = stats_df.filter("stat_name = 'avg_length'").first().stat_value

        inverted_index = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="inverted_index", keyspace="search_engine") \
            .load()

        documents = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="documents", keyspace="search_engine") \
            .load()

        df_map = inverted_index.groupBy("term_text").count() \
            .rdd.map(lambda x: (x.term_text, x["count"])) \
            .collectAsMap()

        df_bc = sc.broadcast(df_map)

        joined_data = inverted_index.join(
            documents,
            "doc_id",
            "inner"
        )

        query_terms = [word.lower() for word in query.split()]
        if not query_terms:
            print("Empty query!")
            return
        filtered_data = joined_data.filter(
            F.col("term_text").isin(query_terms)
        )

        def calculate_bm25(row):
            term = row.term_text
            tf = row.tf
            doc_length = row.length
            df = df_bc.value.get(term, 0)
            idf = math.log((total_docs + 1) / (df + 0.5))
            score = idf * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (doc_length / avg_length)))
            return (row.doc_id, score)


        results = filtered_data.rdd \
            .map(calculate_bm25) \
            .reduceByKey(lambda a, b: a + b) \
            .takeOrdered(10, key=lambda x: -x[1])
        titles = documents.rdd \
            .map(lambda x: (x.doc_id, x.title)) \
            .collectAsMap()
        print("\nTop 10 Results:")
        for i, (doc_id, score) in enumerate(results, 1):
            print(f"{i}. {doc_id}   {titles.get(doc_id, 'Unknown')} [Score: {score:.2f}]")
    finally:
        sc.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit query.py '<query>'")
        sys.exit(1)
    main(sys.argv[1])
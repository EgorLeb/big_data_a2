from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark import StorageLevel


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


n = 1000
df = spark.read.parquet("/a.parquet").select(['id', 'title', 'text']).persist(StorageLevel.MEMORY_AND_DISK).limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)

(df.select(['id', 'title', 'text']).coalesce(1).write.format("csv").option("sep", "\t")
 .option("header", "false").mode("overwrite").save("hdfs:///index/data"))

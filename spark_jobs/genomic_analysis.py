from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

def main():
    spark = SparkSession.builder \
        .appName("Genomic Data Analysis") \
        .getOrCreate()

    # Load data
    df = spark.read.csv("hdfs://namenode:9000/data/genomic_data.csv", header=True)

    # Example transformation: Split a genomic sequence column
    df = df.withColumn("Sequence_Split", split(col("Genomic_Sequence"), ","))

    # Perform analysis (e.g., count of sequences)
    sequence_count = df.select("Sequence_Split").count()
    print(f"Total number of sequences: {sequence_count}")

    spark.stop()

if __name__ == "__main__":
    main()

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Genomic Data ML Model") \
        .getOrCreate()

    # Load and preprocess data
    df = spark.read.csv("hdfs://namenode:9000/data/genomic_data.csv", header=True)
    label_indexer = StringIndexer(inputCol="Label", outputCol="indexedLabel").fit(df)
    assembler = VectorAssembler(inputCols=df.columns[:-1], outputCol="features")

    # Split data
    (training_data, test_data) = df.randomSplit([0.7, 0.3])

    # Define model
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features", numTrees=10)

    # Build pipeline
    pipeline = Pipeline(stages=[label_indexer, assembler, rf])

    # Train model
    model = pipeline.fit(training_data)

    # Evaluate model
    predictions = model.transform(test_data)
    accuracy = predictions.filter(predictions['prediction'] == predictions['indexedLabel']).count() / float(test_data.count())
    print(f"Test Accuracy: {accuracy}")

    spark.stop()

if __name__ == "__main__":
    main()

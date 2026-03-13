import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Windows fix — set HADOOP_HOME automatically
os.environ["HADOOP_HOME"] = "D:\\SoftwareTools\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";D:\\SoftwareTools\\hadoop\\bin"

def get_spark_session(app_name="HackathonPipeline"):
    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_delta(spark, path):
    return spark.read.format("delta").load(path)


def write_delta(df, path, partition_by=None, merge_schema=False):
    writer = df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", str(merge_schema).lower())

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.save(path)
    print(f"Written to Delta Table: {path}")
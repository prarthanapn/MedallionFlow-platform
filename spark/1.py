
import os, sys
sys.path.insert(0, '.')
from spark.delta_utils import get_spark_session, read_delta

spark = get_spark_session('Peek')

print('=== BRONZE (raw) ===')
bronze = read_delta(spark, 'data/delta/bronze')
bronze.select('PassengerId', 'Age', 'Cabin', 'Embarked', '_ingest_timestamp').show(5)

print('=== SILVER (cleaned) ===')
silver = read_delta(spark, 'data/delta/silver')
silver.select('PassengerId', 'Age', 'Cabin', 'Embarked', '_processed_at').show(5)
print('=== NULL COUNT IN SILVER ===')
from pyspark.sql.functions import col, sum as spark_sum
silver.select([spark_sum(col(c).isNull().cast('int')).alias(c) for c in ['Age','Cabin','Embarked']]).show()

spark.stop()

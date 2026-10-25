from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase6_Partitions")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("outputs/cleaned_employees_parquet")

print("Original row count:", df.count())
print("Original partitions:", df.rdd.getNumPartitions())

# 1. Repartition into four partitions
repartitioned = df.repartition(4)

print("\nAfter repartition(4):")
print("Partitions:", repartitioned.rdd.getNumPartitions())
print("Rows:", repartitioned.count())

# 2. Reduce the partition count
coalesced = repartitioned.coalesce(2)

print("\nAfter coalesce(2):")
print("Partitions:", coalesced.rdd.getNumPartitions())
print("Rows:", coalesced.count())

# 3. Repartition using a column
by_department = df.repartition(4, "department")

print("\nAfter repartition(4, department):")
print("Partitions:", by_department.rdd.getNumPartitions())
print("Rows:", by_department.count())

# 4. Cache data that will be reused
cached = df.cache()

print("\nFirst action:", cached.count())
print("Second action:", cached.count())

# 5. Release cached data
cached.unpersist()

# 6. Inspect a physical plan
by_department.groupBy("department").count().explain("formatted")

spark.stop()
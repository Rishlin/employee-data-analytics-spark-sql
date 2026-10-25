from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("Phase6_Parquet")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

source_path = "outputs/cleaned_employees_parquet"
parquet_path = "outputs/phase6_parquet_demo"
partitioned_path = "outputs/phase6_parquet_by_department"
csv_path = "outputs/phase6_csv_demo"

df = spark.read.parquet(source_path)

print("Source rows:", df.count())
df.printSchema()

# 1. Write a Parquet copy with compression
df.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(parquet_path)

# 2. Read it back and validate
reloaded = spark.read.parquet(parquet_path)

print("\nParquet reload rows:", reloaded.count())
print("Parquet reload columns:", len(reloaded.columns))

if df.count() != reloaded.count():
    raise ValueError("Parquet row-count validation failed")

# 3. Select only the columns needed for a query
print("\nSelected columns from Parquet:")
spark.read.parquet(parquet_path) \
    .select("employee_id", "department", "salary_inr") \
    .show(10, truncate=False)

# 4. Filter the Parquet dataset
print("\nEmployees earning above 80000:")
spark.read.parquet(parquet_path) \
    .filter(F.col("salary_inr") > 80000) \
    .select("employee_id", "department", "salary_inr") \
    .show(10, truncate=False)

# 5. Write partitioned Parquet output
df.write.mode("overwrite") \
    .partitionBy("department") \
    .parquet(partitioned_path)

partitioned = spark.read.parquet(partitioned_path)

print("\nPartitioned Parquet rows:", partitioned.count())
print("Partitioned Parquet schema:")
partitioned.printSchema()

# 6. Write a CSV copy for comparison
# Spark writes a directory containing part files, not one named CSV file.
df.write.mode("overwrite") \
    .option("header", True) \
    .csv(csv_path)

print("\nOutput directories created:")
print(parquet_path)
print(partitioned_path)
print(csv_path)

# 7. Show where Spark reads the data from
spark.read.parquet(parquet_path) \
    .select("department", "salary_inr") \
    .filter(F.col("salary_inr") > 80000) \
    .explain("formatted")

spark.stop()
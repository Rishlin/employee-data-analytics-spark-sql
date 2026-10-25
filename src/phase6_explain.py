from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase6_Explain")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("outputs/cleaned_employees_parquet")
df.createOrReplaceTempView("employees")

query = """
    SELECT
        department,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS avg_salary
    FROM employees
    WHERE salary_inr > 50000
    GROUP BY department
    ORDER BY avg_salary DESC
"""

print("\n1. EXPLAIN EXTENDED")
spark.sql("EXPLAIN EXTENDED " + query).show(
    truncate=False
)

print("\n2. EXPLAIN FORMATTED")
spark.sql("EXPLAIN FORMATTED " + query).show(
    truncate=False
)

print("\n3. DataFrame explain")
df.filter(df.salary_inr > 50000) \
  .groupBy("department") \
  .avg("salary_inr") \
  .explain("formatted")

spark.stop()
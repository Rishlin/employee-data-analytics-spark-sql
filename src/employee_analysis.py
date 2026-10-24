
from pathlib import Path
from pyspark.sql import SparkSession

# 1. Locate the project and dataset
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PATH = PROJECT_ROOT / "data" / "employee_analytics_dataset.csv"

# 2. Create a local Spark session
spark = (
    SparkSession.builder
    .appName("Employee Data Analytics")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

# 3. Load the employee dataset
df = spark.read.csv(
    str(DATA_PATH),
    header=True,
    inferSchema=True
)

# 4. Register a Spark SQL view
df.createOrReplaceTempView("employees")

print("===== PHASE 1: SPARK SQL FUNDAMENTALS =====")

# 5. Dataset overview
print("\nTotal employees:")
print(df.count())

print("\nDataset schema:")
df.printSchema()

# 6. Select specific columns
print("\nEmployee details:")
spark.sql("""
    SELECT employee_id, first_name, department, salary_inr
    FROM employees
    LIMIT 5
""").show(truncate=False)

# 7. Filter employees
print("\nEngineering employees:")
spark.sql("""
    SELECT employee_id, first_name, salary_inr
    FROM employees
    WHERE department = 'Engineering'
""").show(truncate=False)

# 8. Sort by salary
print("\nTop 10 highest-paid employees:")
spark.sql("""
    SELECT first_name, department, salary_inr
    FROM employees
    ORDER BY salary_inr DESC
    LIMIT 10
""").show(truncate=False)

# 9. Aggregate salary statistics
print("\nOverall salary statistics:")
spark.sql("""
    SELECT
        COUNT(*) AS total_employees,
        ROUND(AVG(salary_inr), 2) AS average_salary,
        MAX(salary_inr) AS highest_salary,
        MIN(salary_inr) AS lowest_salary,
        SUM(salary_inr) AS total_salary
    FROM employees
""").show(truncate=False)

# 10. Unique departments
print("\nDepartments:")
spark.sql("""
    SELECT DISTINCT department
    FROM employees
    ORDER BY department
""").show(truncate=False)

# 11. Missing-value checks
print("\nMissing-value summary:")
spark.sql("""
    SELECT
        SUM(CASE WHEN salary_inr IS NULL THEN 1 ELSE 0 END)
            AS missing_salaries,
        SUM(CASE WHEN performance_rating IS NULL THEN 1 ELSE 0 END)
            AS missing_ratings,
        SUM(CASE WHEN manager_id IS NULL THEN 1 ELSE 0 END)
            AS missing_manager_ids
    FROM employees
""").show()

# 12. Calculated columns and aliases
print("\nEmployee full names and estimated bonuses:")
spark.sql("""
    SELECT
        CONCAT(first_name, ' ', last_name) AS full_name,
        salary_inr AS salary,
        ROUND(salary_inr * bonus_percentage, 2) AS estimated_bonus
    FROM employees
    LIMIT 5
""").show(truncate=False)

# 13. Stop Spark
spark.stop()


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase6_Joins")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load the existing cleaned dataset
employees = spark.read.parquet("outputs/cleaned_employees_parquet")
employees.createOrReplaceTempView("employees")

print("Total employees:", employees.count())

# 1. INNER JOIN: employees with a matching manager
print("\n1. INNER JOIN")
spark.sql("""
    SELECT
        e.employee_id,
        e.first_name,
        e.department,
        e.manager_id,
        m.first_name AS manager_first_name
    FROM employees e
    INNER JOIN employees m
        ON e.manager_id = m.employee_id
    ORDER BY e.employee_id
""").show(20, truncate=False)

# 2. LEFT JOIN: retain every employee
print("\n2. LEFT JOIN")
spark.sql("""
    SELECT
        e.employee_id,
        e.first_name,
        e.manager_id,
        m.first_name AS manager_first_name
    FROM employees e
    LEFT JOIN employees m
        ON e.manager_id = m.employee_id
    ORDER BY e.employee_id
""").show(20, truncate=False)

# 3. RIGHT JOIN: retain every row on the right side
print("\n3. RIGHT JOIN")
spark.sql("""
    SELECT
        e.employee_id,
        e.first_name,
        m.employee_id AS manager_employee_id,
        m.first_name AS manager_first_name
    FROM employees e
    RIGHT JOIN employees m
        ON e.manager_id = m.employee_id
    ORDER BY m.employee_id
""").show(20, truncate=False)

# 4. FULL OUTER JOIN
print("\n4. FULL OUTER JOIN")
spark.sql("""
    SELECT
        e.employee_id,
        e.first_name,
        m.employee_id AS manager_employee_id,
        m.first_name AS manager_first_name
    FROM employees e
    FULL OUTER JOIN employees m
        ON e.manager_id = m.employee_id
""").show(20, truncate=False)

# 5. CROSS JOIN: limit both inputs to avoid a huge result
print("\n5. CROSS JOIN")
small_employees = spark.sql("""
    SELECT employee_id, first_name
    FROM employees
    ORDER BY employee_id
    LIMIT 3
""")
small_employees.createOrReplaceTempView("small_employees")

small_departments = spark.sql("""
    SELECT DISTINCT department
    FROM employees
    WHERE department IS NOT NULL
    ORDER BY department
    LIMIT 2
""")
small_departments.createOrReplaceTempView("small_departments")

spark.sql("""
    SELECT e.first_name, d.department
    FROM small_employees e
    CROSS JOIN small_departments d
""").show(truncate=False)

# 6. Find employees whose manager is missing or unmatched
print("\n6. Missing or unmatched manager references")
spark.sql("""
    SELECT
        e.employee_id,
        e.first_name,
        e.manager_id
    FROM employees e
    LEFT JOIN employees m
        ON e.manager_id = m.employee_id
    WHERE e.manager_id IS NOT NULL
      AND m.employee_id IS NULL
""").show(truncate=False)

spark.stop()

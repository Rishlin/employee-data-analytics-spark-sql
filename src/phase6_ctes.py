from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase6_CTEs")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("outputs/cleaned_employees_parquet")
df.createOrReplaceTempView("employees")

# 1. Single CTE: departments paying above the overall average
print("\n1. Departments above the overall average salary")

spark.sql("""
    WITH overall AS (
        SELECT AVG(salary_inr) AS overall_avg
        FROM employees
    ),
    department_stats AS (
        SELECT
            department,
            COUNT(*) AS employee_count,
            AVG(salary_inr) AS avg_salary
        FROM employees
        GROUP BY department
    )
    SELECT
        d.department,
        d.employee_count,
        ROUND(d.avg_salary, 2) AS avg_salary,
        ROUND(o.overall_avg, 2) AS overall_avg
    FROM department_stats d
    CROSS JOIN overall o
    WHERE d.avg_salary > o.overall_avg
    ORDER BY d.avg_salary DESC
""").show(truncate=False)

# 2. Multiple CTEs: highest-paid employee per department
print("\n2. Highest-paid employee per department")

spark.sql("""
    WITH department_average AS (
        SELECT
            department,
            AVG(salary_inr) AS avg_salary
        FROM employees
        GROUP BY department
    ),
    ranked_employees AS (
        SELECT
            employee_id,
            first_name,
            last_name,
            department,
            salary_inr,
            ROW_NUMBER() OVER (
                PARTITION BY department
                ORDER BY salary_inr DESC, employee_id
            ) AS rn
        FROM employees
    )
    SELECT
        r.employee_id,
        r.first_name,
        r.last_name,
        r.department,
        r.salary_inr,
        ROUND(a.avg_salary, 2) AS department_avg_salary
    FROM ranked_employees r
    INNER JOIN department_average a
        ON r.department = a.department
    WHERE r.rn = 1
    ORDER BY r.department
""").show(truncate=False)

# 3. Compare a CTE with a subquery
print("\n3. Subquery example")

spark.sql("""
    SELECT department, salary_inr
    FROM employees
    WHERE salary_inr > (
        SELECT AVG(salary_inr)
        FROM employees
    )
    ORDER BY salary_inr DESC
""").show(20, truncate=False)

spark.stop()
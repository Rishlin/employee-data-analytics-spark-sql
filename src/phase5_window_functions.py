
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase5_Window_Functions")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load the cleaned employee dataset
df = spark.read.parquet("outputs/cleaned_employees_parquet")

# Create a temporary SQL view
df.createOrReplaceTempView("employees")

print("PHASE 5: WINDOW FUNCTIONS")
print("Total employees:", df.count())


# Q1: ROW_NUMBER - Number employees by salary
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, salary_inr,
           ROW_NUMBER() OVER (
               ORDER BY salary_inr DESC, employee_id ASC
           ) AS row_num
    FROM employees
""")

print("\nQ1: ROW_NUMBER - Employees by salary")
result.show(20, truncate=False)


# Q2: RANK - Rank all employees by salary
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           salary_inr,
           RANK() OVER (
               ORDER BY salary_inr DESC
           ) AS salary_rank
    FROM employees
""")

print("\nQ2: RANK - Overall salary ranking")
result.show(20, truncate=False)


# Q3: DENSE_RANK - Rank without gaps after ties
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           salary_inr,
           DENSE_RANK() OVER (
               ORDER BY salary_inr DESC
           ) AS salary_dense_rank
    FROM employees
""")

print("\nQ3: DENSE_RANK - Salary ranking")
result.show(20, truncate=False)


# Q4: ROW_NUMBER within each department
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, salary_inr,
           ROW_NUMBER() OVER (
               PARTITION BY department
               ORDER BY salary_inr DESC, employee_id ASC
           ) AS department_row_num
    FROM employees
""")

print("\nQ4: ROW_NUMBER within each department")
result.show(30, truncate=False)


# Q5: RANK employees within each department
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, salary_inr,
           RANK() OVER (
               PARTITION BY department
               ORDER BY salary_inr DESC
           ) AS department_salary_rank
    FROM employees
""")

print("\nQ5: Department salary ranking")
result.show(30, truncate=False)


# Q6: DENSE_RANK within each department
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, salary_inr,
           DENSE_RANK() OVER (
               PARTITION BY department
               ORDER BY salary_inr DESC
           ) AS department_dense_rank
    FROM employees
""")

print("\nQ6: Department dense ranking")
result.show(30, truncate=False)


# Q7: LAG - Compare salary with previous employee
# Order employees by hire date
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           hire_date, salary_inr,
           LAG(salary_inr) OVER (
               ORDER BY hire_date ASC, employee_id ASC
           ) AS previous_employee_salary
    FROM employees
""")

print("\nQ7: LAG - Previous row salary by hire date")
result.show(20, truncate=False)


# Q8: LEAD - Compare salary with next employee
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           hire_date, salary_inr,
           LEAD(salary_inr) OVER (
               ORDER BY hire_date ASC, employee_id ASC
           ) AS next_employee_salary
    FROM employees
""")

print("\nQ8: LEAD - Next row salary by hire date")
result.show(20, truncate=False)


# Q9: Salary difference from previous employee
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           hire_date, salary_inr,
           LAG(salary_inr) OVER (
               ORDER BY hire_date ASC, employee_id ASC
           ) AS previous_salary,
           salary_inr - LAG(salary_inr) OVER (
               ORDER BY hire_date ASC, employee_id ASC
           ) AS salary_difference
    FROM employees
""")

print("\nQ9: Salary difference from previous row")
result.show(20, truncate=False)


# Q10: Running salary total across employees
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           salary_inr,
           SUM(salary_inr) OVER (
               ORDER BY employee_id
               ROWS BETWEEN UNBOUNDED PRECEDING
               AND CURRENT ROW
           ) AS running_salary_total
    FROM employees
""")

print("\nQ10: Running salary total")
result.show(20, truncate=False)


# Q11: Running salary total within each department
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, salary_inr,
           SUM(salary_inr) OVER (
               PARTITION BY department
               ORDER BY employee_id
               ROWS BETWEEN UNBOUNDED PRECEDING
               AND CURRENT ROW
           ) AS department_running_total
    FROM employees
""")

print("\nQ11: Department running salary total")
result.show(30, truncate=False)


# Q12: Highest-paid employee in each department
result = spark.sql("""
    WITH ranked_employees AS (
        SELECT employee_id, first_name, last_name,
               department, salary_inr,
               ROW_NUMBER() OVER (
                   PARTITION BY department
                   ORDER BY salary_inr DESC, employee_id ASC
               ) AS rn
        FROM employees
    )
    SELECT employee_id, first_name, last_name,
           department, salary_inr
    FROM ranked_employees
    WHERE rn = 1
    ORDER BY department
""")

print("\nQ12: Highest-paid employee in each department")
result.show(truncate=False)


# Q13: Top 3 employees by salary in each department
result = spark.sql("""
    WITH ranked_employees AS (
        SELECT employee_id, first_name, last_name,
               department, salary_inr,
               ROW_NUMBER() OVER (
                   PARTITION BY department
                   ORDER BY salary_inr DESC, employee_id ASC
               ) AS rn
        FROM employees
    )
    SELECT employee_id, first_name, last_name,
           department, salary_inr, rn
    FROM ranked_employees
    WHERE rn <= 3
    ORDER BY department, rn
""")

print("\nQ13: Top 3 employees in each department")
result.show(truncate=False)


# Q14: All employees tied for the highest salary in each department
result = spark.sql("""
    WITH ranked_employees AS (
        SELECT employee_id, first_name, last_name,
               department, salary_inr,
               RANK() OVER (
                   PARTITION BY department
                   ORDER BY salary_inr DESC
               ) AS salary_rank
        FROM employees
    )
    SELECT employee_id, first_name, last_name,
           department, salary_inr, salary_rank
    FROM ranked_employees
    WHERE salary_rank = 1
    ORDER BY department, employee_id
""")

print("\nQ14: Highest salary including ties")
result.show(truncate=False)


# Q15: Compare each employee's salary with department average
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, salary_inr,
           ROUND(
               AVG(salary_inr) OVER (
                   PARTITION BY department
               ), 2
           ) AS department_average_salary,
           ROUND(
               salary_inr - AVG(salary_inr) OVER (
                   PARTITION BY department
               ), 2
           ) AS difference_from_department_average
    FROM employees
    ORDER BY department, salary_inr DESC
""")

print("\nQ15: Salary compared with department average")
result.show(30, truncate=False)


# End Spark session
spark.stop()

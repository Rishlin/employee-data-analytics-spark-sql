
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Phase4_Business_Questions")
    .master("local[*]")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load the cleaned employee dataset
df = spark.read.parquet("outputs/cleaned_employees_parquet")

# Create a temporary SQL view
df.createOrReplaceTempView("employees")

print("Cleaned employee dataset loaded successfully!")
print("Total employees:", df.count())

df.printSchema()


# Question 1: Average salary by department
result = spark.sql("""
    SELECT department,
           ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY department
    ORDER BY average_salary DESC
""")
print("\nQ1: Average salary by department")
result.show(truncate=False)


# Question 2: Total salary expenditure by department
result = spark.sql("""
    SELECT department,
           SUM(salary_inr) AS total_salary
    FROM employees
    GROUP BY department
    ORDER BY total_salary DESC
""")
print("\nQ2: Total salary by department")
result.show(truncate=False)


# Question 3: Highest and lowest salary by department
result = spark.sql("""
    SELECT department,
           MAX(salary_inr) AS highest_salary,
           MIN(salary_inr) AS lowest_salary
    FROM employees
    GROUP BY department
""")
print("\nQ3: Highest and lowest salary by department")
result.show(truncate=False)


# Question 4: Employees earning above 80000
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, salary_inr
    FROM employees
    WHERE salary_inr > 80000
    ORDER BY salary_inr DESC
""")
print("\nQ4: Employees earning above 80000")
result.show(truncate=False)


# Question 5: Overall salary difference
result = spark.sql("""
    SELECT MAX(salary_inr) - MIN(salary_inr)
           AS salary_difference
    FROM employees
""")
print("\nQ5: Overall salary difference")
result.show()


# Question 6: Employee count by department
result = spark.sql("""
    SELECT department,
           COUNT(*) AS employee_count
    FROM employees
    GROUP BY department
    ORDER BY employee_count DESC
""")
print("\nQ6: Employee count by department")
result.show(truncate=False)


# Question 7: Departments with more than 20 employees
result = spark.sql("""
    SELECT department,
           COUNT(*) AS employee_count
    FROM employees
    GROUP BY department
    HAVING COUNT(*) > 20
    ORDER BY employee_count DESC
""")
print("\nQ7: Departments with more than 20 employees")
result.show(truncate=False)


# Question 8: Average salary by job title
result = spark.sql("""
    SELECT job_title,
           ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY job_title
    ORDER BY average_salary DESC
""")
print("\nQ8: Average salary by job title")
result.show(truncate=False)


# Question 9: Employee count by employment type
result = spark.sql("""
    SELECT employment_type,
           COUNT(*) AS employee_count
    FROM employees
    GROUP BY employment_type
    ORDER BY employee_count DESC
""")
print("\nQ9: Employee count by employment type")
result.show(truncate=False)


# Question 10: Employee count by location
result = spark.sql("""
    SELECT location,
           COUNT(*) AS employee_count
    FROM employees
    GROUP BY location
    ORDER BY employee_count DESC
""")
print("\nQ10: Employee count by location")
result.show(truncate=False)


# Question 11: Average salary for each experience level
result = spark.sql("""
    SELECT years_experience,
           ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY years_experience
    ORDER BY years_experience
""")
print("\nQ11: Average salary by experience")
result.show(truncate=False)


# Question 12: Compare salary across experience groups
result = spark.sql("""
    SELECT
        CASE
            WHEN years_experience < 3 THEN '0-2 years'
            WHEN years_experience < 6 THEN '3-5 years'
            WHEN years_experience < 11 THEN '6-10 years'
            ELSE '11+ years'
        END AS experience_group,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY
        CASE
            WHEN years_experience < 3 THEN '0-2 years'
            WHEN years_experience < 6 THEN '3-5 years'
            WHEN years_experience < 11 THEN '6-10 years'
            ELSE '11+ years'
        END
    ORDER BY average_salary DESC
""")
print("\nQ12: Salary by experience group")
result.show(truncate=False)


# Question 13: More than 5 years of experience and salary above 70000
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           years_experience, salary_inr
    FROM employees
    WHERE years_experience > 5
      AND salary_inr > 70000
    ORDER BY salary_inr DESC
""")
print("\nQ13: Experienced employees earning above 70000")
result.show(truncate=False)


# Question 14: Average experience by department
result = spark.sql("""
    SELECT department,
           ROUND(AVG(years_experience), 2) AS average_experience
    FROM employees
    GROUP BY department
    ORDER BY average_experience DESC
""")
print("\nQ14: Average experience by department")
result.show(truncate=False)


# Question 15: Average bonus percentage by department
result = spark.sql("""
    SELECT department,
           ROUND(AVG(bonus_percentage) * 100, 2)
               AS average_bonus_percent
    FROM employees
    GROUP BY department
    ORDER BY average_bonus_percent DESC
""")
print("\nQ15: Average bonus percentage by department")
result.show(truncate=False)


# Question 16: Count employees by performance rating
result = spark.sql("""
    SELECT performance_rating,
           COUNT(*) AS employee_count
    FROM employees
    GROUP BY performance_rating
    ORDER BY performance_rating
""")
print("\nQ16: Employee count by performance rating")
result.show(truncate=False)


# Question 17: Average salary by performance rating
result = spark.sql("""
    SELECT performance_rating,
           ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    WHERE performance_rating IS NOT NULL
    GROUP BY performance_rating
    ORDER BY performance_rating DESC
""")
print("\nQ17: Average salary by performance rating")
result.show(truncate=False)


# Question 18: Employees with performance rating 5
result = spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, performance_rating
    FROM employees
    WHERE performance_rating = 5
""")
print("\nQ18: Employees with rating 5")
result.show(truncate=False)


# Question 19: Missing performance ratings by department
result = spark.sql("""
    SELECT department,
           COUNT(*) AS missing_rating_count
    FROM employees
    WHERE performance_rating IS NULL
    GROUP BY department
    ORDER BY missing_rating_count DESC
""")
print("\nQ19: Missing ratings by department")
result.show(truncate=False)


# Question 20: Employees hired each year
result = spark.sql("""
    SELECT YEAR(hire_date) AS hire_year,
           COUNT(*) AS employees_hired
    FROM employees
    GROUP BY YEAR(hire_date)
    ORDER BY hire_year
""")
print("\nQ20: Employees hired each year")
result.show(truncate=False)


# Question 21: Most recent hire date by department
result = spark.sql("""
    SELECT department,
           MAX(hire_date) AS most_recent_hire_date
    FROM employees
    GROUP BY department
    ORDER BY most_recent_hire_date DESC
""")
print("\nQ21: Most recent hire by department")
result.show(truncate=False)


# Question 22: Average salary by education level
result = spark.sql("""
    SELECT education_level,
           ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    WHERE education_level IS NOT NULL
    GROUP BY education_level
    ORDER BY average_salary DESC
""")
print("\nQ22: Average salary by education level")
result.show(truncate=False)


# Question 23: Department and employment type breakdown
result = spark.sql("""
    SELECT department, employment_type,
           COUNT(*) AS employee_count
    FROM employees
    GROUP BY department, employment_type
    ORDER BY department, employee_count DESC
""")
print("\nQ23: Department and employment type")
result.show(truncate=False)


# Question 24: Number of employees per manager
result = spark.sql("""
    SELECT manager_id,
           COUNT(*) AS employees_managed
    FROM employees
    WHERE manager_id IS NOT NULL
    GROUP BY manager_id
    ORDER BY employees_managed DESC
""")
print("\nQ24: Employees per manager")
result.show(truncate=False)


# Question 25: Employees earning above their department average
result = spark.sql("""
    WITH department_salary AS (
        SELECT department,
               AVG(salary_inr) AS avg_salary
        FROM employees
        GROUP BY department
    )
    SELECT e.employee_id, e.first_name, e.last_name,
           e.department, e.salary_inr,
           ROUND(d.avg_salary, 2) AS department_average
    FROM employees e
    JOIN department_salary d
      ON e.department = d.department
    WHERE e.salary_inr > d.avg_salary
    ORDER BY e.department, e.salary_inr DESC
""")
print("\nQ25: Employees earning above department average")
result.show(truncate=False)


# Stop Spark after all questions finish
spark.stop()




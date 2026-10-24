
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

# ============================================================
# PHASE 2: EMPLOYEE DATA ANALYTICS
# ============================================================

# TASK 1: EMPLOYEE DISTRIBUTION

print("\n========== TASK 1: EMPLOYEE DISTRIBUTION ==========")

print("\n--- Employees by Department ---")
spark.sql("""
    SELECT department, COUNT(*) AS employee_count
    FROM employees
    GROUP BY department
    ORDER BY employee_count DESC
""").show(truncate=False)

print("\n--- Employees by Location ---")
spark.sql("""
    SELECT location, COUNT(*) AS employee_count
    FROM employees
    GROUP BY location
    ORDER BY employee_count DESC
""").show(truncate=False)

print("\n--- Employees by Employment Type ---")
spark.sql("""
    SELECT employment_type, COUNT(*) AS employee_count
    FROM employees
    GROUP BY employment_type
    ORDER BY employee_count DESC
""").show(truncate=False)

print("\n--- Top 10 Job Titles ---")
spark.sql("""
    SELECT job_title, COUNT(*) AS employee_count
    FROM employees
    GROUP BY job_title
    ORDER BY employee_count DESC
    LIMIT 10
""").show(truncate=False)


# TASK 2: SALARY ANALYSIS

print("\n========== TASK 2: SALARY ANALYSIS ==========")

print("\n--- Salary Statistics by Department ---")
spark.sql("""
    SELECT
        department,
        COUNT(salary_inr) AS employees_with_salary,
        ROUND(AVG(salary_inr), 2) AS average_salary,
        MIN(salary_inr) AS minimum_salary,
        MAX(salary_inr) AS maximum_salary,
        SUM(salary_inr) AS total_salary
    FROM employees
    GROUP BY department
    ORDER BY average_salary DESC
""").show(truncate=False)

print("\n--- Top 10 Highest-Paid Employees ---")
spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, job_title, salary_inr
    FROM employees
    ORDER BY salary_inr DESC
    LIMIT 10
""").show(truncate=False)

print("\n--- Employees Earning Above Company Average ---")
spark.sql("""
    SELECT employee_id, first_name, last_name,
           department, salary_inr
    FROM employees
    WHERE salary_inr > (
        SELECT AVG(salary_inr) FROM employees
    )
    ORDER BY salary_inr DESC
""").show(truncate=False)


# TASK 3: JOB-TITLE ANALYSIS

print("\n========== TASK 3: JOB-TITLE ANALYSIS ==========")

print("\n--- Employee Count and Average Salary by Job Title ---")
spark.sql("""
    SELECT
        job_title,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY job_title
    ORDER BY average_salary DESC
""").show(truncate=False)

print("\n--- Job Titles Within Departments ---")
spark.sql("""
    SELECT
        department,
        job_title,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY department, job_title
    ORDER BY department, average_salary DESC
""").show(truncate=False)


# TASK 4: EXPERIENCE ANALYSIS

print("\n========== TASK 4: EXPERIENCE ANALYSIS ==========")

print("\n--- Average Experience by Department ---")
spark.sql("""
    SELECT
        department,
        ROUND(AVG(years_experience), 2) AS average_experience
    FROM employees
    GROUP BY department
    ORDER BY average_experience DESC
""").show(truncate=False)

print("\n--- Salary by Experience Level ---")
spark.sql("""
    SELECT
        CASE
            WHEN years_experience < 2 THEN 'Beginner'
            WHEN years_experience < 5 THEN 'Intermediate'
            WHEN years_experience < 10 THEN 'Experienced'
            ELSE 'Senior Experience'
        END AS experience_level,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY
        CASE
            WHEN years_experience < 2 THEN 'Beginner'
            WHEN years_experience < 5 THEN 'Intermediate'
            WHEN years_experience < 10 THEN 'Experienced'
            ELSE 'Senior Experience'
        END
    ORDER BY average_salary DESC
""").show(truncate=False)

print("\n--- Salary by Exact Years of Experience ---")
spark.sql("""
    SELECT
        years_experience,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY years_experience
    ORDER BY years_experience
""").show(truncate=False)


# TASK 5: PERFORMANCE ANALYSIS

print("\n========== TASK 5: PERFORMANCE ANALYSIS ==========")

print("\n--- Employees by Performance Rating ---")
spark.sql("""
    SELECT performance_rating, COUNT(*) AS employee_count
    FROM employees
    GROUP BY performance_rating
    ORDER BY performance_rating
""").show(truncate=False)

print("\n--- Salary by Performance Rating ---")
spark.sql("""
    SELECT
        performance_rating,
        COUNT(*) AS rated_employees,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    WHERE performance_rating IS NOT NULL
    GROUP BY performance_rating
    ORDER BY performance_rating
""").show(truncate=False)

print("\n--- Missing Performance Ratings ---")
spark.sql("""
    SELECT COUNT(*) AS missing_performance_ratings
    FROM employees
    WHERE performance_rating IS NULL
""").show()

print("\n--- Performance by Department ---")
spark.sql("""
    SELECT
        department,
        ROUND(AVG(performance_rating), 2) AS average_rating,
        COUNT(performance_rating) AS rated_employees
    FROM employees
    GROUP BY department
    ORDER BY average_rating DESC
""").show(truncate=False)


# TASK 6: EDUCATION ANALYSIS

print("\n========== TASK 6: EDUCATION ANALYSIS ==========")

print("\n--- Employees by Education Level ---")
spark.sql("""
    SELECT education_level, COUNT(*) AS employee_count
    FROM employees
    GROUP BY education_level
    ORDER BY employee_count DESC
""").show(truncate=False)

print("\n--- Salary by Education Level ---")
spark.sql("""
    SELECT
        education_level,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY education_level
    ORDER BY average_salary DESC
""").show(truncate=False)

print("\n--- Education, Experience and Salary ---")
spark.sql("""
    SELECT
        education_level,
        ROUND(AVG(years_experience), 2) AS average_experience,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY education_level
    ORDER BY average_salary DESC
""").show(truncate=False)


# TASK 7: HIRING TREND ANALYSIS

print("\n========== TASK 7: HIRING TRENDS ==========")

print("\n--- Convert Hiring Dates ---")
spark.sql("""
    SELECT
        employee_id,
        hire_date,
        TO_DATE(hire_date, 'dd-MM-yyyy') AS converted_hire_date,
        YEAR(TO_DATE(hire_date, 'dd-MM-yyyy')) AS hire_year
    FROM employees
    LIMIT 10
""").show(truncate=False)

print("\n--- Employees Hired Each Year ---")
spark.sql("""
    SELECT
        YEAR(TO_DATE(hire_date, 'dd-MM-yyyy')) AS hire_year,
        COUNT(*) AS employees_hired
    FROM employees
    WHERE TO_DATE(hire_date, 'dd-MM-yyyy') IS NOT NULL
    GROUP BY YEAR(TO_DATE(hire_date, 'dd-MM-yyyy'))
    ORDER BY hire_year
""").show(truncate=False)

print("\n--- Employees Hired Each Month ---")
spark.sql("""
    SELECT
        DATE_FORMAT(
            TO_DATE(hire_date, 'dd-MM-yyyy'),
            'yyyy-MM'
        ) AS hiring_month,
        COUNT(*) AS employees_hired
    FROM employees
    WHERE TO_DATE(hire_date, 'dd-MM-yyyy') IS NOT NULL
    GROUP BY DATE_FORMAT(
        TO_DATE(hire_date, 'dd-MM-yyyy'),
        'yyyy-MM'
    )
    ORDER BY hiring_month
""").show(truncate=False)


# TASK 8: COMBINED BUSINESS ANALYSIS

print("\n========== TASK 8: COMBINED BUSINESS ANALYSIS ==========")

print("\n--- Department Workforce and Salary Report ---")
spark.sql("""
    SELECT
        department,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS average_salary,
        MIN(salary_inr) AS minimum_salary,
        MAX(salary_inr) AS maximum_salary,
        ROUND(AVG(years_experience), 2) AS average_experience
    FROM employees
    GROUP BY department
    ORDER BY employee_count DESC
""").show(truncate=False)

print("\n--- Department and Employment Type Comparison ---")
spark.sql("""
    SELECT
        department,
        employment_type,
        COUNT(*) AS employee_count,
        ROUND(AVG(salary_inr), 2) AS average_salary
    FROM employees
    GROUP BY department, employment_type
    ORDER BY department, employee_count DESC
""").show(truncate=False)

print("\n--- Top 10 Estimated Bonuses ---")
spark.sql("""
    SELECT
        employee_id,
        first_name,
        last_name,
        department,
        salary_inr,
        bonus_percentage,
        ROUND(salary_inr * bonus_percentage / 100, 2)
            AS estimated_bonus
    FROM employees
    ORDER BY estimated_bonus DESC
    LIMIT 10
""").show(truncate=False)

print("\n--- Employees Earning Above Department Average ---")
spark.sql("""
    SELECT
        employee_id,
        first_name,
        last_name,
        department,
        salary_inr
    FROM (
        SELECT
            employee_id,
            first_name,
            last_name,
            department,
            salary_inr,
            AVG(salary_inr) OVER (
                PARTITION BY department
            ) AS department_average_salary
        FROM employees
    ) department_analysis
    WHERE salary_inr > department_average_salary
    ORDER BY department, salary_inr DESC
""").show(truncate=False)


# TASK 9: FINAL VALIDATION

print("\n========== TASK 9: FINAL VALIDATION ==========")

print("Total employees:", df.count())
print("Total columns:", len(df.columns))

spark.sql("""
    SELECT COUNT(DISTINCT department) AS total_departments
    FROM employees
""").show()

print("\nPHASE 2 ANALYSIS COMPLETED")

spark.stop()

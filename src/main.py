
from datetime import datetime

from pyspark.sql import functions as F

from analytics_utils import (
    create_spark_session,
    load_cleaned_data,
    validate_employee_data,
    save_dataframe_as_csv,
)


def main():
    spark = None

    try:
        print("=" * 55)
        print("EMPLOYEE DATA ANALYTICS - MAIN WORKFLOW")
        print("=" * 55)

        # Step 1: Create Spark session
        spark = create_spark_session("EmployeeAnalyticsMain")
        print("\n[1/5] Spark session created successfully.")

        # Step 2: Load cleaned employee data
        employees = load_cleaned_data(spark)
        print("[2/5] Cleaned employee data loaded.")

        # Step 3: Validate the dataset
        report = validate_employee_data(employees)

        print("\nDATA VALIDATION REPORT")
        for key, value in report.items():
            print(f"{key}: {value}")

        # Step 4: Run business analyses
        department_summary = (
            employees
            .groupBy("department")
            .agg(
                F.count("*").alias("employee_count"),
                F.round(F.avg("salary_inr"), 2)
                .alias("average_salary_inr"),
                F.max("salary_inr").alias("highest_salary_inr"),
                F.min("salary_inr").alias("lowest_salary_inr"),
            )
            .orderBy(F.desc("average_salary_inr"))
        )

        top_paid_employees = (
            employees
            .filter(F.col("salary_inr").isNotNull())
            .select(
                "employee_id",
                "first_name",
                "last_name",
                "department",
                "salary_inr",
            )
            .orderBy(F.desc("salary_inr"))
            .limit(10)
        )

        print("\n[3/5] Department salary analysis:")
        department_summary.show(truncate=False)

        print("\nTop 10 highest-paid employees:")
        top_paid_employees.show(truncate=False)

        # Step 5: Save outputs using a unique run identifier
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        department_path = save_dataframe_as_csv(
            department_summary,
            f"department_summary_{run_id}",
        )

        top_paid_path = save_dataframe_as_csv(
            top_paid_employees,
            f"top_paid_employees_{run_id}",
        )

        print("\n[4/5] Analysis results saved successfully.")
        print(f"Department summary: {department_path}")
        print(f"Top-paid employees: {top_paid_path}")

        print("\n[5/5] WORKFLOW COMPLETED SUCCESSFULLY!")

    except Exception as error:
        print(f"\nWORKFLOW FAILED: {error}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            print("\nSpark session stopped.")


if __name__ == "__main__":
    main()

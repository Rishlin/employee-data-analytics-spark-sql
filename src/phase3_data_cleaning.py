
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# PHASE 3: DATA CLEANING AND QUALITY VALIDATION
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = PROJECT_DIR / "data" / "employee_analytics_dataset.csv"
OUTPUT_DIR = PROJECT_DIR / "outputs"
CLEANED_PATH = OUTPUT_DIR / "cleaned_employees_parquet"
REPORT_PATH = OUTPUT_DIR / "phase3_quality_report.txt"

if not DATA_PATH.exists():
    raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def count_missing_values(df):
    """Count NULL and blank values in every column."""
    counts = {}

    for column_name, data_type in df.dtypes:
        column = F.col(column_name)

        if data_type == "string":
            condition = column.isNull() | (F.trim(column) == "")
        else:
            condition = column.isNull()

        counts[column_name] = df.filter(condition).count()

    return counts


def print_missing_comparison(before, after):
    print("\n" + "=" * 75)
    print("MISSING VALUES: BEFORE VS AFTER CLEANING")
    print("=" * 75)
    print(f"{'Column':<25}{'Before':>12}{'After':>12}{'Reduced':>12}")

    for name, before_count in before.items():
        after_count = after.get(name, 0)
        print(
            f"{name:<25}{before_count:>12}"
            f"{after_count:>12}{before_count - after_count:>12}"
        )


spark = (
    SparkSession.builder
    .appName("EmployeeDataAnalytics_Phase3_DataCleaning")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    # TASK 1: INSPECT RAW DATA
    print("\n" + "=" * 65)
    print("TASK 1: RAW DATASET INSPECTION")
    print("=" * 65)

    raw_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(DATA_PATH))
    )

    raw_df.createOrReplaceTempView("raw_employees")

    initial_rows = raw_df.count()
    initial_columns = len(raw_df.columns)
    before_missing = count_missing_values(raw_df)

    print("Original rows:", initial_rows)
    print("Original columns:", initial_columns)
    print("Columns:", raw_df.columns)
    raw_df.printSchema()
    raw_df.show(5, truncate=False)

    # TASK 2: MISSING VALUES
    print("\nTASK 2: MISSING VALUE ANALYSIS")
    for name, count in before_missing.items():
        print(f"{name}: {count}")

    # TASK 3: DUPLICATES
    print("\nTASK 3: DUPLICATE CHECKS")

    duplicate_ids = (
        raw_df.filter(F.col("employee_id").isNotNull())
        .groupBy("employee_id")
        .count()
        .filter(F.col("count") > 1)
    )
    duplicate_id_groups = duplicate_ids.count()
    print("Duplicate employee ID groups:", duplicate_id_groups)
    duplicate_ids.show(50, truncate=False)

    duplicate_rows = (
        raw_df.groupBy(*raw_df.columns)
        .count()
        .filter(F.col("count") > 1)
    )
    fully_duplicated_groups = duplicate_rows.count()
    print("Fully duplicated row groups:", fully_duplicated_groups)
    duplicate_rows.show(50, truncate=False)

    # TASK 4: PERFORMANCE RATINGS
    print("\nTASK 4: PERFORMANCE RATING QUALITY")

    if "performance_rating" in raw_df.columns:
        raw_df.agg(
            F.sum(
                F.when(F.col("performance_rating").isNull(), 1)
                .otherwise(0)
            ).alias("missing_ratings"),
            F.count("performance_rating").alias("available_ratings"),
            F.min("performance_rating").alias("minimum_rating"),
            F.max("performance_rating").alias("maximum_rating"),
        ).show()

        invalid_ratings = raw_df.filter(
            F.col("performance_rating").isNotNull()
            & (
                (F.col("performance_rating") < 1)
                | (F.col("performance_rating") > 5)
            )
        )
        print("Ratings outside the assumed 1-5 scale:")
        invalid_ratings.show(50, truncate=False)

    # TASK 5: HIRE DATE VALIDATION
    print("\nTASK 5: HIRE DATE VALIDATION")

    if "hire_date" in raw_df.columns:
        parsed_df = raw_df.withColumn(
            "hire_date_parsed",
            F.try_to_timestamp(
                F.col("hire_date"), F.lit("dd-MM-yyyy")
            ).cast("date"),
        )

        invalid_dates = parsed_df.filter(
            F.col("hire_date").isNotNull()
            & (F.trim(F.col("hire_date")) != "")
            & F.col("hire_date_parsed").isNull()
        )

        parsed_df.select("hire_date", "hire_date_parsed").show(
            10, truncate=False
        )
        print("Invalid non-empty hire dates:", invalid_dates.count())
        invalid_dates.select("employee_id", "hire_date").show(
            50, truncate=False
        )

    # TASK 6: BUSINESS RULES
    print("\nTASK 6: BUSINESS RULE VALIDATION")

    checks = {
        "Negative salary": ("salary_inr", lambda c: F.col(c) < 0),
        "Negative experience": (
            "years_experience", lambda c: F.col(c) < 0
        ),
        "Negative bonus percentage": (
            "bonus_percentage", lambda c: F.col(c) < 0
        ),
        "Bonus percentage above 100%": (
            "bonus_percentage", lambda c: F.col(c) > 1
        ),
    }

    for label, (column_name, condition_builder) in checks.items():
        if column_name in raw_df.columns:
            invalid_df = raw_df.filter(
                F.col(column_name).isNotNull()
                & condition_builder(column_name)
            )
            print(f"{label}: {invalid_df.count()} record(s)")
            invalid_df.select(
                *[
                    c for c in (
                        "employee_id", column_name,
                        "department", "job_title"
                    )
                    if c in raw_df.columns
                ]
            ).show(20, truncate=False)

    # TASK 7: MANAGER ID VALIDATION
    print("\nTASK 7: MANAGER ID VALIDATION")

    employee_ids = (
        raw_df.filter(
            F.col("employee_id").isNotNull()
            & (F.trim(F.col("employee_id")) != "")
        )
        .select(F.trim(F.col("employee_id")).alias("employee_id"))
        .distinct()
    )

    manager_ids = (
        raw_df.filter(
            F.col("manager_id").isNotNull()
            & (F.trim(F.col("manager_id")) != "")
        )
        .select(F.trim(F.col("manager_id")).alias("manager_id"))
        .distinct()
    )

    orphan_managers = manager_ids.alias("m").join(
        employee_ids.alias("e"),
        F.col("m.manager_id") == F.col("e.employee_id"),
        "left_anti",
    )

    orphan_manager_count = orphan_managers.count()
    print("Distinct non-missing manager IDs:", manager_ids.count())
    print("Manager IDs not found among employees:", orphan_manager_count)

    if orphan_manager_count:
        orphan_managers.show(50, truncate=False)
    else:
        print("PASS: All recorded manager IDs reference employees.")

    print("Missing manager IDs are reported as missing, not as orphans.")

    # TASK 8: CLEAN DATA
    print("\nTASK 8: BUILD CLEANED DATASET")

    cleaned_df = raw_df

    for field in raw_df.schema.fields:
        if field.dataType.simpleString() == "string":
            name = field.name
            cleaned_df = cleaned_df.withColumn(
                name,
                F.when(
                    F.trim(F.col(name)) == "",
                    F.lit(None).cast("string"),
                ).otherwise(F.trim(F.col(name))),
            )

    if "hire_date" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn(
            "hire_date",
            F.try_to_timestamp(
                F.col("hire_date"), F.lit("dd-MM-yyyy")
            ).cast("date"),
        )

    cleaned_df.createOrReplaceTempView("cleaned_employees")

    # TASK 9: FINAL COUNTS AND MISSING-VALUE COMPARISON
    print("\nTASK 9: FINAL VALIDATION")

    cleaned_rows = cleaned_df.count()
    cleaned_columns = len(cleaned_df.columns)
    after_missing = count_missing_values(cleaned_df)

    print("Original rows:", initial_rows)
    print("Cleaned rows:", cleaned_rows)
    print("Original columns:", initial_columns)
    print("Cleaned columns:", cleaned_columns)

    print_missing_comparison(before_missing, after_missing)

    cleaned_df.printSchema()
    cleaned_df.show(10, truncate=False)

    # Save cleaned Parquet separately from the raw CSV.
    cleaned_df.write.mode("overwrite").parquet(str(CLEANED_PATH))

    reloaded_df = spark.read.parquet(str(CLEANED_PATH))
    reloaded_rows = reloaded_df.count()
    reloaded_columns = len(reloaded_df.columns)

    print("Reloaded Parquet rows:", reloaded_rows)
    print("Reloaded Parquet columns:", reloaded_columns)

    # TASK 10: WRITE FINAL QUALITY REPORT
    checks_passed = (
        initial_rows == cleaned_rows == reloaded_rows
        and initial_columns == cleaned_columns == reloaded_columns
        and duplicate_id_groups == 0
        and fully_duplicated_groups == 0
        and orphan_manager_count == 0
    )

    report_lines = [
        "PHASE 3 - FINAL DATA QUALITY REPORT",
        "=" * 45,
        f"Original rows: {initial_rows}",
        f"Cleaned rows: {cleaned_rows}",
        f"Reloaded Parquet rows: {reloaded_rows}",
        f"Original columns: {initial_columns}",
        f"Cleaned columns: {cleaned_columns}",
        f"Reloaded Parquet columns: {reloaded_columns}",
        f"Duplicate employee ID groups: {duplicate_id_groups}",
        f"Fully duplicated row groups: {fully_duplicated_groups}",
        f"Orphan manager IDs: {orphan_manager_count}",
        "",
        "MISSING VALUES: BEFORE -> AFTER",
    ]

    for name, before_count in before_missing.items():
        report_lines.append(
            f"{name}: {before_count} -> {after_missing.get(name, 0)}"
        )

    report_lines.extend([
        "",
        "CLEANING POLICY",
        "- Text is trimmed; blank text becomes NULL.",
        "- hire_date is converted to a date.",
        "- Missing values are not guessed or imputed.",
        "- Duplicate records are reported, not automatically deleted.",
        "- Missing manager IDs are not counted as orphan IDs.",
        "",
        f"STRUCTURAL CHECKS: {'PASS' if checks_passed else 'REVIEW REQUIRED'}",
        "Review business-rule violations and missing-value counts above.",
    ])

    REPORT_PATH.write_text("\n".join(report_lines), encoding="utf-8")

    print("\nQuality report saved to:", REPORT_PATH)
    print("\n".join(report_lines))

    if not checks_passed:
        print("\nSome structural checks need investigation before committing.")
    else:
        print("\nStructural checks passed. Review the full report before committing.")

    print("\nPHASE 3 DATA CLEANING COMPLETED")

finally:
    spark.stop()

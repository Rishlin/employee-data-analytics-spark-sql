
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"
CLEANED_DATA_PATH = OUTPUT_DIR / "cleaned_employees_parquet"
ANALYSIS_OUTPUT_PATH = OUTPUT_DIR / "analysis_results"


def create_spark_session(app_name="EmployeeDataAnalytics"):
    """Create and return a Spark session."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    return spark


def load_cleaned_data(spark):
    """Load the cleaned employee dataset from Parquet."""
    if not CLEANED_DATA_PATH.exists():
        raise FileNotFoundError(
            f"Cleaned dataset not found: {CLEANED_DATA_PATH}. "
            "Run phase3_data_cleaning.py first."
        )

    df = spark.read.parquet(str(CLEANED_DATA_PATH))

    if df.isEmpty():
        raise ValueError("The cleaned employee dataset is empty.")

    return df


def validate_employee_data(df):
    """Perform basic structural and data quality checks."""
    required_columns = [
        "employee_id",
        "department",
        "salary_inr",
        "years_experience",
        "hire_date",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {missing_columns}"
        )

    invalid_salary_count = df.filter(
        F.col("salary_inr").isNotNull()
        & (F.col("salary_inr") < 0)
    ).count()

    if invalid_salary_count > 0:
        raise ValueError(
            f"Found {invalid_salary_count} negative salary values."
        )

    return {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "invalid_salary_count": invalid_salary_count,
    }


def save_dataframe_as_csv(df, output_name):
    """Save a DataFrame as a CSV output directory."""
    if not output_name or Path(output_name).name != output_name:
        raise ValueError("output_name must be a simple folder name.")

    output_path = ANALYSIS_OUTPUT_PATH / output_name

    if output_path.exists():
        raise FileExistsError(
            f"Output already exists: {output_path}. "
            "Choose another name or remove the old output intentionally."
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .option("header", True)
        .mode("errorifexists")
        .csv(str(output_path))
    )

    return output_path

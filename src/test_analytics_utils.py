
from analytics_utils import (
    create_spark_session,
    load_cleaned_data,
    validate_employee_data,
)

spark = None

try:
    spark = create_spark_session("TestAnalyticsUtils")

    df = load_cleaned_data(spark)

    print("\n--- Dataset Preview ---")
    df.show(5, truncate=False)

    print("\n--- Data Validation ---")
    results = validate_employee_data(df)

    for key, value in results.items():
        print(f"{key}: {value}")

    print("\nReusable functions tested successfully!")

except (FileNotFoundError, ValueError, Exception) as error:
    print(f"Test failed: {error}")
    raise

finally:
    if spark is not None:
        spark.stop()

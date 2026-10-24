from pyspark.sql import SparkSession

# Step 1: Create a Spark session
spark = SparkSession.builder \
    .appName("Employee Data Analytics") \
    .master("local[*]") \
    .getOrCreate()

# Step 2: Load the employee dataset
file_path = r"C:\sparkdata\employee_analytics_dataset.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

# Step 3: Display basic information
print("Total number of employees:", df.count())

print("Dataset Schema:")
df.printSchema()

print("First 5 employee records:")
df.show(5, truncate=False)


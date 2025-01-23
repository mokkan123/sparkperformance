from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read10GBCSV") \
    .getOrCreate()

# Input path
input_path = "/opt/data/10gb_data.csv"  # Replace with your data location

# Read the CSV data
df = spark.read.option("header", "true").csv(input_path)

# Perform a simple transformation (e.g., filtering and aggregation)
result_df = df.filter(df["category"] == "A") \
              .groupBy("category") \
              .count()

# Show the result
result_df.show()

# Optional: Write the result to disk
output_path = "/opt/data/output/result.csv"
result_df.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Processed data written to {output_path}")

# Stop the SparkSession
spark.stop()


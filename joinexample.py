from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("JoinExample").getOrCreate()

# Create the first dataset: Employees
employees_data = [
    (1, "Alice", 1001),
    (2, "Bob", 1002),
    (3, "Cathy", 1003),
    (4, "David", None)  # David does not belong to a department
]

employees_columns = ["EmpID", "Name", "DeptID"]

employees_df = spark.createDataFrame(employees_data, schema=employees_columns)

# Create the second dataset: Departments
departments_data = [
    (1001, "HR"),
    (1002, "Finance"),
    (1003, "Engineering"),
    (1004, "Sales")  # No employee belongs to Sales
]

departments_columns = ["DeptID", "DeptName"]

departments_df = spark.createDataFrame(departments_data, schema=departments_columns)

# Show the datasets
print("Employees DataFrame:")
employees_df.show()

print("Departments DataFrame:")
departments_df.show()

# Perform an inner join
print("Inner Join (only matching rows):")
inner_join_df = employees_df.join(departments_df, on="DeptID", how="inner")
inner_join_df.show()

# Perform a left outer join
print("Left Outer Join (all employees, even if no department):")
left_outer_join_df = employees_df.join(departments_df, on="DeptID", how="left")
left_outer_join_df.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, sum, to_timestamp
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder.appName("SeasonalTrendsAnalysis").getOrCreate()

# Load the data
source_path = "Input file location"
df = spark.read.csv(source_path, header = True, inferSchema = True)


df = df.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"))


# Calculate TotalPrice as Quantity VS UnitPrice
df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

# Extract month from InvoiceDate
df = df.withColumn("Month", month(col("InvoiceDate")))
# Aggregate TotalSales by month
monthly_sales = df.groupBy("Month").agg(sum("TotalPrice").alias("TotalMonthlySales"))
monthly_sales = monthly_sales.orderBy("Month")



monthly_sales_pd = monthly_sales.toPandas()

plt.figure(figsize=(10, 6))
plt.plot(monthly_sales_pd["Month"], monthly_sales_pd["TotalMonthlySales"], marker='o')
plt.title("Monthly Sales Trends")
plt.xlabel("Month")
plt.ylabel("Total Monthly Sales")
plt.xticks(range(1, 13))
plt.grid(True)
plt.show()


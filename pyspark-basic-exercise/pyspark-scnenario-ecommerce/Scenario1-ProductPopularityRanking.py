from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, expr
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName("ProductPopularityRanking").getOrCreate()

# Load the Table
print("Reading Dataframe---------------------")
source_path = "Input-Path"
df = spark.read.csv(source_path, header = True, inferSchema = True)

print("Reading COmpleted---------------------")
# Create a total price column
df = df.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

# Group by StockCode and Country, calculating TotalQuantity and TotalSales
popularity_df = df.groupBy("StockCode","Country").agg(sum("Quantity").alias("TotalQuantity"),sum("TotalPrice").alias("TotalSales"))

# # Define weights (1:1 for simplicity, adjust based on importance)
popularity_df = popularity_df.withColumn("PopularityScore", expr("0.5 * TotalQuantity + 0.5 * TotalSales"))

# Define a window by Country, ordering by PopularityScore descending
window_spec = Window.partitionBy("Country").orderBy(col("PopularityScore").desc())

# Apply row_number to rank products within each country
ranked_df = popularity_df.withColumn("Rank", row_number().over(window_spec))

# Filter to get top 5 products within each country
top_n_products = ranked_df.filter(col("Rank") <= 5)

output_path = "Output-Path"
top_n_products.write.mode("overwrite").parquet(output_path)
print("------Write-Completed------")
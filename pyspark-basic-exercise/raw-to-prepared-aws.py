from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("ReadFromS3Raw_WriteToPreparedS3").getOrCreate()

print("Reading data from S3...")
source_path = "YourS3DataLocation"
output_path = "OutputLocationS3"

print("Data read successfully")
df = spark.read.csv(source_path, header = True, inferSchema = True)

selected_df = df.select('InvoiceNo','StockCode','Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country')

print("Writing data to S3 as Parquet...")
selected_df.write.mode('overwrite').parquet(output_path)
print("Data written successfully")

spark.stop()
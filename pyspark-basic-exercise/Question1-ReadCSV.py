from pyspark.sql import SparkSession

# Initialize spark session
spark = SparkSession.builder.appName("ReadCSV_Parquet").getOrCreate()

# Read Data from CSV
df = spark.read.csv('path/to/input.csv',header = True, inferSchema = True)

selected_df = df.select('InvoiceNo','StockCode','Description','Quantity','InvoiceDate','UnitPrice','CustomerID','Country')

# selected_df.write.parquet('/Volumes/OneTouch/Datasets/KaggleDatasets/Sales.parquet')
selected_df.write.parquet('path/to/output.parquet')
print('-------------Write Completed---------------')
# Stop the Spark Session
spark.stop()
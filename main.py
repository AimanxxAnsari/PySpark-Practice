from pyspark.sql import SparkSession

sc = SparkSession\
    .builder\
    .master("local[*]")\
    .appName('example_spark')\
    .getOrCreate()

data = [
    (1, "Gilbert Gathara", "24", "Nairobi"), 
    (2, "Someone Else", "32", "Nowhere")
]
headers = ("id", "Name", "Age", "Location")
df = sc.createDataFrame(data, headers)
df.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Trip Data").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("FATAL")

dbname = "tripdata"
tblname = "ny_taxi"
ny_taxi = spark.sql(f'SELECT * FROM {dbname}.{tblname}')
ny_taxi = ny_taxi.withColumn('trip_duration', col('lpep_dropoff_datetime').cast('long') - col('lpep_pickup_datetime').cast('long'))
ny_taxi.select('trip_duration', 'trip_distance', 'total_amount').summary().show()
ny_taxi.filter(col('total_amount') < 0).select('total_amount').count()
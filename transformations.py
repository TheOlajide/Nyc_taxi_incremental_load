import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder\
        .appName("parquet_to_csv")\
        .master("local[*]")\
        .getOrCreate()

#january data
jan = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-01.parquet")

jan.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

#february data
feb = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-02.parquet")

feb.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")


#march data
mar = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-03.parquet")

mar.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

#april data

apr = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-04.parquet")

apr.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

#may data

may = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-05.parquet")

may.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

# june data

jun = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-06.parquet")

jun.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

# july data

jul = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-07.parquet")

jul.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

# august data

aug = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-08.parquet")

aug.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

#september data

sept = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-09.parquet")

sept.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

#october data

oct = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-10.parquet")

oct.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

#november data

nov = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-11.parquet")

nov.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")

#december 

dec = spark.read\
  .format("parquet")\
  .load("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/yellow_tripdata_2024-12.parquet")

dec.write\
   .format("csv")\
   .mode("overwrite")\
   .save("C:/Users/AbdullAteef/Downloads/yellow_taxi_dataset/to_csv")


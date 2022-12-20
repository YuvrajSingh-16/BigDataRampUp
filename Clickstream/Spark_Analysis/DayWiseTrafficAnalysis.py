import sys
import findspark

## Setting spark home
findspark.init("/usr/local/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col


try:
    ## Creating Spark session
    spark = SparkSession.builder.config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*").enableHiveSupport().getOrCreate()

    ## Reading clickstream table from hive warehouse(Partitions & Buckets)
    df = spark.read.json("hdfs://localhost:9000/user/hive/warehouse/clickstream_db.db/clickstream/")

    print("[+] Loading data into Dataframe...") 

    ## Showing dataframe
    df.show()

    ## Printing Schema
    df.printSchema()

    ## Creating Day of week column from logdate
    df2 = df.withColumn("DayOfWeek", date_format(col("logdate"), "EEEE")).select(col("sessionid"), col("DayOfWeek"))

    ## Dataframe groupBy DayOfWeek
    final_df = df2.groupBy("DayOfWeek").count().sort(col("count").desc())

    ## Saving Dataframe into Hive table
    final_df.write.mode("overwrite").saveAsTable("clickstream_db.daywise_traffic_analysis")

    print("[+] Day Wise Traffic Analysis data stores in hive table successfully...!!!\n[+] Completed!")

    sys.exit()

except Exception as e:
    print(e)
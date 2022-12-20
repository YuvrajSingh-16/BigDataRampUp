import sys
import findspark

## Setting spark home
findspark.init("/usr/local/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


try:
    ## Creating Spark session
    spark = SparkSession.builder.config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*").enableHiveSupport().getOrCreate()

    ## Reading clickstream table form hive warehouse(Partitions & Buckets)
    df = spark.read.json("hdfs://localhost:9000/user/hive/warehouse/clickstream_db.db/clickstream/")

    print("[+] Loading data into dataframe..") 

    ## Showing dataframe
    df.show()

    ## Printing Schema
    df.printSchema()

    df.createOrReplaceTempView("df") 		##before query execution on dataframe create view to avoid error

    ## Analysis using Dataframe
    print("[+] Creating view of actions for table join query...")

    ## Saperate Dataframe on the basis of actions
    df_grouped_view = df.filter("action == 'view'").groupBy(col("userid")).count().withColumnRenamed("count", "view_count").cache()
    df_grouped_addtocart = df.filter("action == 'addtocart'").groupBy(col("userid")).count().withColumnRenamed("count", "addtocart_count").cache()
    df_grouped_removefromcart = df.filter("action == 'removefromcart'").groupBy(col("userid")).count().withColumnRenamed("count", "removefromcart_count").cache()
    df_grouped_purchase = df.filter("action == 'purchase'").groupBy(col("userid")).count().withColumnRenamed("count", "purchase_count").cache()

    ## Creating TEMP views for each action
    df_grouped_view.createOrReplaceTempView("df_grouped_view")
    df_grouped_addtocart.createOrReplaceTempView("df_grouped_addtocart")
    df_grouped_removefromcart.createOrReplaceTempView("df_grouped_removefromcart")
    df_grouped_purchase.createOrReplaceTempView("df_grouped_purchase")

    ## Applying join
    df_final = df_grouped_view.join(df_grouped_addtocart, ["userid"]).join(df_grouped_removefromcart, ["userid"]).join(df_grouped_purchase, ["userid"])

    ## Saving Dataframe into Hive table
    df_final.write.mode("overwrite").saveAsTable("clickstream_db.user_cart_analysis")

    print("[+] UserCartAnalysis data stored into Hive table successfully...!!!\n[+] Completed!")
    sys.exit()

except Exception as e:
    print(e)
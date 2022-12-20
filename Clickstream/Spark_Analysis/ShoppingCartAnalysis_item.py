import sys
import findspark

## Setting spark home
findspark.init("/usr/local/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring_index


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
    df_items = df.withColumn("Item", substring_index(col("url"), "?", -1)).cache()

    print("[+] Creating view of actions for table join query...")

    ## Saperate Dataframe on the basis of actions
    df_grouped_view = df_items.filter("action == 'view'").groupBy(col("Item")).count().withColumnRenamed("count", "view_count").cache()
    df_grouped_addtocart = df_items.filter("action == 'addtocart'").groupBy(col("Item")).count().withColumnRenamed("count", "addtocart_count").cache()
    df_grouped_removefromcart = df_items.filter("action == 'removefromcart'").groupBy(col("Item")).count().withColumnRenamed("count", "removefromcart_count").cache()
    df_grouped_purchase = df_items.filter("action == 'purchase'").groupBy(col("Item")).count().withColumnRenamed("count", "purchase_count").cache()

    ## Creating TEMP views for each action
    df_grouped_view.createOrReplaceTempView("df_grouped_view")
    df_grouped_addtocart.createOrReplaceTempView("df_grouped_addtocart")
    df_grouped_removefromcart.createOrReplaceTempView("df_grouped_removefromcart")
    df_grouped_purchase.createOrReplaceTempView("df_grouped_purchase")

    ## Applying join
    df_final = df_grouped_view.join(df_grouped_addtocart, ["item"]).join(df_grouped_removefromcart, ["item"]).join(df_grouped_purchase, ["item"])

    ## Saving Dataframe into Hive table
    df_final.write.mode("overwrite").saveAsTable("clickstream_db.shopping_cart_analysis")

    ### Analysis using SparkSQL
    ## Setting Property to run create table using script
    # spark.sql("SET spark.sql.legacy.createHiveTableByDefault=false")

    ## Use clickstream_db and create table
    # spark.sql("USE clickstream_db")
    # spark.sql("create temp view action_view as select SUBSTRING_INDEX(df.url, '?', -1) as item ,count(df.action) as view_count from df where df.action='view' group By df.url order By view_count desc")
    # spark.sql("create temp view action_addtocart as select SUBSTRING_INDEX(df.url, '?', -1) as item, count(df.action) as addtocart_count from df where df.action='addtocart' group By df.url order By addtocart_count desc")
    # spark.sql("create temp view action_removefromcart as select SUBSTRING_INDEX(df.url, '?', -1) as item, count(df.action) as removefromcart_count from df where df.action='removefromcart' group By df.url order By removefromcart_count desc")
    # spark.sql("create temp view action_purchase as select SUBSTRING_INDEX(df.url, '?', -1) as item, count(df.action) as purchase_count from df where df.action='purchase' group By df.url order By purchase_count desc")


    # spark.sql("TRUNCATE TABLE shopping_cart_analysis")

    # ## Creating shopping_cart_analysis table using OUTER JOIN
    # spark.sql("INSERT INTO TABLE shopping_cart_analysis SELECT a1.item, a1.view_count, a2.addtocart_count, a3.removefromcart_count, a4.purchase_count FROM action_view as a1 FULL OUTER JOIN action_addtocart as a2 ON (a1.item = a2.item) FULL OUTER JOIN action_removefromcart as a3 ON (a1.item = a3.item) FULL OUTER JOIN action_purchase as a4 ON (a1.item = a4.item) where a1.item is not null")

    print("[+] ShoppingCartAnalysis_item data stored into Hive table successfully...!!!\n[+] Completed!")
    sys.exit()

except Exception as e:
    print(e)
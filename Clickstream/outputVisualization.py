#!/bin/python3
#===================================================================================================================
#
#          FILE:  outputVisualization.py
#
#         USAGE:  python3 outputVisualization.py 
#
#   DESCRIPTION: Runs Clickstream Analysis Visualization
#
#       OPTIONS:  ---
#  REQUIREMENTS:  Java8, Hadoop-2.10.1, Hive-2.3.9, PySpark-3.3.0, Python3
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  Yuvraj Singh Kiraula, yuvraj.kiraula@impetus.com
#       COMPANY:  Impetus Technologies
#       VERSION:  2.0
#       CREATED:  06/12/2022 20:06 PM IST
#      REVISION:  ---
#===================================================================================================================

import findspark
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as tck

from decouple import Config, RepositoryEnv, config as cnf


## Setting spark home
findspark.init("/usr/local/spark")
from pyspark.sql import SparkSession


def create_n_save_plot(df, x, y, x_label, y_label, title, img_name, n=5):
    # Creating subplot
    f, ax = plt.subplots(1)

    ## Dividing data into x & y
    x_data = list(df.head(n)[f"{x}"])
    y_data = list(df.head(n)[f"{y}"])

    ax.grid(zorder=0, axis='y')

    ax.grid(which='minor', linestyle=':', linewidth=0.5, axis='y')
    # Make the minor ticks and gridlines show.
    ax.minorticks_on()
    ax.tick_params(axis='x', which='minor', bottom=False)


    ## Create bar chart out of x & y data
    ax.bar(x_data, y_data, zorder=3)

    ## Setting y limit
    ax.set_ylim(ymin=(max(y_data)*94)/100, ymax=max(y_data)+(max(y_data)*2)/100)

    ## Setting labels & title
    ax.set_ylabel(y_label, size=12, fontweight="bold")
    ax.set_xlabel(x_label, size=12, fontweight="bold")
    ax.set_title(title, size=16,fontweight="bold")

    ## Saving image
    f.savefig(f"{CLICKSTREAM_HOME}/visualization_page/images/{img_name}", bbox_inches='tight')



def create_n_save_multibar_graph(shopping_cart_analysis_df, x, x_label, title, img_name):

    ## Number of multibar graphs
    N = 5
    ind = np.arange(N)
    ## Width for each multibar 
    width = 0.20

    ## Creating list of each action count
    view_count = list(shopping_cart_analysis_df["view_count"].head())
    addtocart_count = list(shopping_cart_analysis_df["addtocart_count"].head())
    removefromcart_count = list(shopping_cart_analysis_df["removefromcart_count"].head())
    purchase_count = list(shopping_cart_analysis_df["purchase_count"].head())

    ## Items list
    items = list(shopping_cart_analysis_df[x].head())

    plt.grid(zorder=0, axis='y')

    plt.grid(which='minor', linestyle=':', linewidth=0.5, axis='y')
    # Make the minor ticks and gridlines show.
    plt.minorticks_on()
    plt.tick_params(axis='x', which='minor', bottom=False)


    ## Barplot for each action
    bar1 = plt.bar(ind, view_count, width, zorder=3)
    bar2 = plt.bar(ind+width, addtocart_count, width, zorder=3)
    bar3 = plt.bar(ind+width*2, removefromcart_count, width, zorder=3)
    bar4 = plt.bar(ind+width*3, purchase_count, width, zorder=3)

    ## Labels and title
    plt.xlabel(x_label, size=12, fontweight="bold")
    plt.ylabel("Action Count", size=12, fontweight="bold")
    plt.title(title, size=16, fontweight="bold")

    ## Setting X ticks
    plt.xticks(ind+width, items)
    ## Setting Legend for each action
    plt.legend((bar1, bar2, bar3, bar4), ('view_count', 'addtocart_count', 'removefromcart_count', 'purchase_count'), loc='upper right')

    ## Setting y limit
    mx = max(max(view_count), max(addtocart_count), max(removefromcart_count), max(purchase_count))
    plt.ylim((mx*70)/100, mx+(mx*12)/100)
    # plt.ylim(9000, 11500)

    ## Saving image
    plt.savefig(f"{CLICKSTREAM_HOME}/visualization_page/images/{img_name}", bbox_inches='tight')



if __name__ == "__main__":
    ## Importing Env Variables
    CLICKSTREAM_HOME = cnf('CLICKSTREAM_HOME', default="/home/hadoopusr/BigDataCaseStudy/Clickstream")

    ## Creating Spark session
    spark = SparkSession.builder.config("spark.sql.hive.metastore.jars", "/usr/local/hive/lib/*").enableHiveSupport().getOrCreate()

    spark.sql("SET spark.sql.legacy.createHiveTableByDefault=false")

    ## Use clickstream_db 
    spark.sql("USE clickstream_db")


    ## Creating Dataframe from hive tables
    activeusers_df = spark.sql("SELECT * FROM clickstream_db.activeusers").toPandas()
    activeusers_df.sort_values(by=['count', 'userid'], ascending=[False, True], inplace=True)

    dayWiseTraffic_df = spark.sql("SELECT * FROM clickstream_db.daywise_traffic_analysis").toPandas()
    useritemvisit_df = spark.sql("SELECT * FROM clickstream_db.useritemvisit").toPandas()

    locationwisetraffic_df = spark.sql("SELECT * FROM clickstream_db.locationwisetraffic").toPandas()
    locationwisetraffic_df.sort_values(by='traffic_count', ascending=False, inplace=True)

    ### Shopping Cart Analysis
    shopping_cart_analysis_df = spark.sql("SELECT * FROM clickstream_db.shopping_cart_analysis").toPandas()
    ## Adding Sum column to the dataframe
    shopping_cart_analysis_df["Sum"] = shopping_cart_analysis_df.iloc[:,1:].sum(axis=1)
    shopping_cart_analysis_df.sort_values(by='Sum', ascending=False, inplace=True)

    ### User Cart Aanlysis
    user_action_analysis_df = spark.sql("SELECT * FROM clickstream_db.user_cart_analysis").toPandas()
    ## Adding Sum column to the dataframe
    user_action_analysis_df["Sum"] = user_action_analysis_df.iloc[:,1:].sum(axis=1)
    user_action_analysis_df.sort_values(by=['Sum', 'userid'], ascending=[False, True], inplace=True)

    print("[+] Dataframes creation done..")

    ## Creating and saving plot for each DF
    create_n_save_multibar_graph(shopping_cart_analysis_df, "Item", "Item", "Top 5 Most Active Items", "top5_mostActiveItems.png")
    create_n_save_multibar_graph(user_action_analysis_df, "userid", "UserID", "Top 5 Most Active User Actions", "top5_mostActiveUsersAction.png")

    create_n_save_plot(activeusers_df, "userid", "count", "UserIDs", "Activity Count", "Top 5 Most Active Users", "top5_activeusers.png")
    create_n_save_plot(dayWiseTraffic_df, "DayOfWeek", "count", "Days", "Traffic Count", "Top 5 Most Active Days", "dayWiseTrafficAnalysis.png")
    create_n_save_plot(useritemvisit_df, "product", "count", "Items", "Visits Count", "Top 5 Most Visited Items", "top5_visitedItems.png")
    create_n_save_plot(locationwisetraffic_df, "location", "traffic_count", "Location", "Traffic Count", "Top 5 Most Active Locations", "top5_mostActiveLocations.png")

    print("[+] Plotting done...")

    print("[+] Completed!")
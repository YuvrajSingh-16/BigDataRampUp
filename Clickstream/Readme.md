Before running this project make sure:
1. You have given dependencies installed:
    - Java8 
    - Hadoop-2.10.1
    - Hive-2.3.9
    - Spark-3.3.0
    - PySpark-3.3.0
    - Python-3.10.6
    - Scala 2.12.15
2. You have started your hadoop clusters:
    - start-dfs.sh && start-yarn.sh
3. You have folder structure ready, all the files and directories required:
    CLICKSTREAM_HOME = /home/hadoopusr/BigDataCaseStudy/Clickstream/
    
    CLICKSTREAM_HOME/
        - archive_helper/
            - Archive_utils.py
            - archiveData.py
            - checkArchives.py
        - evns/
            - dev.env
            - cron.env
        - Hive_Analysis/
            - MostActiveUsers.hql
            - UserItemVisitAnalysis.hql
        - jars/
            - DataGenerator.jar
            - MapReduce_LocationWiseAnalysis.jar
        - listsData/
            - items.txt
            - locations.txt
            - paymentMethods.txt
            - users.txt
        - logs/
        - Spark_Analysis
            - DayWiseTrafficAnalysis.py
            - ShoppingCartAnalysis_item.py
            - UserActionAnalysis.py
        - visualization_page/
            - images/
            - index.html
        - Clickstream_analysis_script.sh
        - Clickstream_analysis.py
        - DataArchiving.sh
        - insertIntoClickstreamTable.hql
        - MR_out_to_hiveTable.hql
        - outputVisualization.py
        - urlopener.sh

4. You have installed all the Python3 dependencies from requirements.txt
5. Database with name clickstream_db and following tables with given schema :-
    - activeusers 
        +-----------+------------+----------+
        | col_name  | data_type  | comment  |
        +-----------+------------+----------+
        | userid    | string     |          |
        | count     | bigint     |          |
        +-----------+------------+----------+

    - daywise_traffic_analysis  
        +------------+------------+----------+
        |  col_name  | data_type  | comment  |
        +------------+------------+----------+
        | dayofweek  | string     |          |
        | count      | bigint     |          |
        +------------+------------+----------+
    
    - clickstream
        +--------------------------+-----------------------+-----------------------+
        |         col_name         |       data_type       |        comment        |
        +--------------------------+-----------------------+-----------------------+
        | userid                   | string                | from deserializer     |
        | sessionid                | string                | from deserializer     |
        | action                   | string                | from deserializer     |
        | url                      | string                | from deserializer     |
        | logtime                  | string                | from deserializer     |
        | payment_method           | string                | from deserializer     |
        | logdate                  | string                | from deserializer     |
        | location                 | string                |                       |
        |                          | NULL                  | NULL                  |
        | # Partition Information  | NULL                  | NULL                  |
        | # col_name               | data_type             | comment               |
        |                          | NULL                  | NULL                  |
        | location                 | string                |                       |
        +--------------------------+-----------------------+-----------------------+
    
    - locationwisetraffic       
        +----------------+------------+----------+
        |    col_name    | data_type  | comment  |
        +----------------+------------+----------+
        | location       | string     |          |
        | traffic_count  | int        |          |
        +----------------+------------+----------+

    - shopping_cart_analysis    
        +-----------------------+------------+----------+
        |       col_name        | data_type  | comment  |
        +-----------------------+------------+----------+
        | item                  | string     |          |
        | view_count            | bigint     |          |
        | addtocart_count       | bigint     |          |
        | removefromcart_count  | bigint     |          |
        | purchase_count        | bigint     |          |
        +-----------------------+------------+----------+

    - user_cart_analysis        
        +-----------------------+------------+----------+
        |       col_name        | data_type  | comment  |
        +-----------------------+------------+----------+
        | userid                | string     |          |
        | view_count            | bigint     |          |
        | addtocart_count       | bigint     |          |
        | removefromcart_count  | bigint     |          |
        | purchase_count        | bigint     |          |
        +-----------------------+------------+----------+

    - useritemvisit             
        +-----------+------------+----------+
        | col_name  | data_type  | comment  |
        +-----------+------------+----------+
        | product   | string     |          |
        | count     | bigint     |          |
        +-----------+------------+----------+

5. Run Clickstream_analysis_script.sh OR Clickstream_analysis.py to start the analysis
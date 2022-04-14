# Databricks notebook source
# MAGIC %md
# MAGIC ####Python Notebook for SCD_06 Implementation in PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC #####Import Libraries 

# COMMAND ----------

from pyspark.sql.functions import when,lit,array_contains,col
from pyspark.sql import SparkSession, SQLContext
import datetime
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #####Spark Session

# COMMAND ----------

spark = SparkSession.builder.appName('SCD_06').getOrCreate()
spark

# COMMAND ----------

# MAGIC %md
# MAGIC #####Reading Stores Initial Data Files into DataFrame 

# COMMAND ----------

# DataFrame
stores_df = spark.read.csv('/FileStore/tables/stores_1.csv', header=True, inferSchema=True)
display(stores_df)

# COMMAND ----------

stores_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Renaming Columns

# COMMAND ----------

stores_df = stores_df.withColumnRenamed("store_city","current_city").withColumnRenamed("store_country","current_country")
display(stores_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Implemeting SCD_06 Required Columns in DataFrame 

# COMMAND ----------

stores_df = stores_df.withColumn("previous_city",stores_df["current_city"]).withColumn("previous_country",stores_df["current_country"]).withColumn("start_date",lit(datetime.datetime(2022, 4, 10).strftime('%Y/%m/%d'))).withColumn("end_date",lit(datetime.datetime(9999, 12, 31).strftime('%Y/%m/%d'))).withColumn("current_flag",lit('Y'))

#Rearrange Columns
stores_df = stores_df.select("store_id", "store_name", "current_city","previous_city", "current_country", "previous_country", "start_date", "end_date", "current_flag")

display(stores_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Saving DataFrame to a Hive Table

# COMMAND ----------

#Save DataFrame to a Temp Table/View
stores_df.createOrReplaceTempView('stores_tmp')

#Initializing SQL Context 
sql_ctx = SQLContext(spark)

#Truncating Table Directory
dbutils.fs.rm("dbfs:/user/hive/warehouse/", True)

#Drop Table if Exists
sql_ctx.sql("DROP TABLE IF EXISTS Stores_scd") 
sql_ctx.sql("CREATE TABLE Stores_scd AS SELECT * from stores_tmp")

# COMMAND ----------

#Reading Data from Table

scd_df = spark.read.table("Stores_scd")
display(scd_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading Incremented Stores Data into DataFrame

# COMMAND ----------

incremented_df = spark.read.csv('/FileStore/tables/stores_2.csv', header=True, inferSchema=True)
display(incremented_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Implementing SCD_06 Calculations.

# COMMAND ----------

#Read Store_ids of Initials Stores Data
stores_list = list(scd_df.toPandas()['store_id'])

for row in incremented_df.collect():
    
    #Update the Old Rows and Mark Current_Flag as FALSE  
    if row['store_id'] in stores_list: 
        scd_df = scd_df.withColumn("current_city", when((scd_df["store_id"]  == row["store_id"]) & (scd_df["store_name"]  == row["store_name"]), row["store_city"]).otherwise(scd_df["current_city"]))
        scd_df = scd_df.withColumn("current_country", when((scd_df["store_id"]  == row["store_id"]) & (scd_df["store_name"]  == row["store_name"]), row["store_country"]).otherwise(scd_df["current_country"]))
        scd_df = scd_df.withColumn("end_date", when((scd_df["store_id"]  == row["store_id"]) & (scd_df["store_name"]  == row["store_name"]), lit(datetime.datetime(2022, 4, 12).strftime('%Y/%m/%d'))).otherwise(scd_df["end_date"]))
        scd_df = scd_df.withColumn("current_flag", when((scd_df["store_id"]  == row["store_id"]) & (scd_df["store_name"]  == row["store_name"]), lit("N")).otherwise(scd_df["current_flag"]))
        
        
    #Insert New Row and Mark Current_Flag as True    
    new_row = [(row["store_id"],row["store_name"],row["store_city"],row["store_city"],row["store_country"],row["store_country"],datetime.datetime(2022, 4, 12).strftime('%Y/%m/%d'),datetime.datetime(9999, 12, 31).strftime('%Y/%m/%d'), 'Y')]
    columns = ["store_id", "store_name", "current_city","previous_city", "current_country", "previous_country", "start_date", "end_date", "current_flag"]
    newdf = spark.createDataFrame(new_row, columns)
    scd_df = scd_df.union(newdf)


# COMMAND ----------

# MAGIC %md
# MAGIC #####Sort and save DataFrame

# COMMAND ----------

scd_df = scd_df.sort(col("store_id"), col("current_flag").asc())
display(scd_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Save Resules to SCD Table

# COMMAND ----------

scd_df.write.mode("overwrite").saveAsTable("Stores_scd")
display(spark.read.table("Stores_scd"))

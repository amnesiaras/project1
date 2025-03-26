# Databricks notebook source
spark.version

df = spark.read.csv('dbfs:/FileStore/hotel_bookings.csv', inferSchema=True, header=True, sep=',')

display(df)

df.printSchema()

# MAGIC %md
# MAGIC Delete Unwanted columns

delete_columns = ["lead_time","arrival_date_year","arrival_date_month","arrival_date_week_number","arrival_date_day_of_month","distribution_channel","is_repeated_guest","previous_cancellations","previous_bookings_not_canceled","assigned_room_type","booking_changes","days_in_waiting_list","adr","required_car_parking_spaces","total_of_special_requests","reservation_status"]
df = df.drop(*delete_columns)

# MAGIC %md
# MAGIC Wrap up Columns
from pyspark.sql.functions import to_date, col, current_date, year, round, when

df = df.withColumn("total_stay",col("stays_in_weekend_nights")+col("stays_in_week_nights"))
df = df.drop("stays_in_weekend_nights","stays_in_week_nights")


df = df.withColumn("travel_type", when((col("adults") + col("children") + col("babies")) > 1 , "family").otherwise("alone"))
df = df.drop("adults","children","babies")


df = df.withColumn("agent_company_id", when(col("agent").isNull() ,col("company")).otherwise(col("agent")))
df.filter((df.agent_company_id.isNull()) | (df.agent_company_id == "NULL")).count()


df = df.withColumn("agent_company_id", when((df.agent_company_id.isNull()) | (df.agent_company_id == "NULL"), "-").otherwise(col("agent_company_id")))
df = df.drop("agent","company")

df = df.withColumn("reservation_status_date", to_date(col("reservation_status_date")))

display(df)

# MAGIC %md
# MAGIC Create table

# MAGIC %sql
# MAGIC drop table if exists default.hotel_info;
# MAGIC
# MAGIC create external table default.hotel_info
# MAGIC (
# MAGIC   hotel string,
# MAGIC   is_canceled integer,
# MAGIC   meal string,
# MAGIC   country string,
# MAGIC   market_segment string,
# MAGIC   reserved_room_type string,
# MAGIC   deposit_type string,
# MAGIC   customer_type string,
# MAGIC   reservation_status_date date,
# MAGIC   total_stay integer,
# MAGIC   travel_type string,
# MAGIC   agent_company_id string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/FileStore/tables/hotel_info';


# MAGIC %md
# MAGIC Write dataframe into specified path of table

df.write.format('delta').mode('overwrite').save('dbfs:/FileStore/tables/hotel_info')

# MAGIC %md
# MAGIC Query table with Spark SQL

read_sql = spark.sql("select * from default.hotel_info")
display(read_sql)


# DBTITLE 1,Delta Lake travel back time
df.write.format('delta').mode('append').save('dbfs:/FileStore/tables/hotel_info')

read_df.count()

# MAGIC %sql
# MAGIC 
# MAGIC describe history default.member_scoring

# read data version 1
spark.read.format("delta").option("versionAsOf", "1").load('dbfs:/FileStore/tables/member_scoring').count()


# MAGIC %sql
# MAGIC RESTORE TABLE default.member_scoring TO VERSION AS OF 1
# data back to version 1

# result >> data version 1
read_df = spark.sql("select * from default.member_scoring")
read_df.count()
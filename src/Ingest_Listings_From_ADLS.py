# Databricks notebook source
dbutils.widgets.text("city","boston","CityName")
dbutils.widgets.text("date","03242024","Date of Load")

# COMMAND ----------

city = dbutils.widgets.get("city")
date =dbutils.widgets.get("date")
print(f"Loading data for {city} for date {date}")


# COMMAND ----------

core_input_path= 'abfss://inbounddata@airbnbstorageaccount.dfs.core.windows.net'
input_path=f'{core_input_path}/{city}/{date}/'
print(f"loading data from {input_path}")

# COMMAND ----------

acc_name = dbutils.secrets.get(scope="airbnb_az",key='storage_account_name')
storage_key = dbutils.secrets.get(scope="airbnb_az",key='storage_key')
spark.conf.set(
    f"fs.azure.account.key.{acc_name}.dfs.core.windows.net",storage_key)

# COMMAND ----------

display(dbutils.fs.ls(input_path))

# COMMAND ----------

listings = f'{input_path}/listings.csv'
listings_df = spark.read.format('csv').option("header",True).load(listings)

# COMMAND ----------

display(listings_df)

# COMMAND ----------

import pyspark.sql.functions as F
listings_df = listings_df.withColumn('city_name',F.lit(city))
display(listings_df)

# COMMAND ----------

listings_df = listings_df.withColumn('load_date',F.lit(date))
display(listings_df)

# COMMAND ----------

listings_df.write.mode('overwrite').saveAsTable('taproot_training.airbnb.listings')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from taproot_training.airbnb.listings

# COMMAND ----------



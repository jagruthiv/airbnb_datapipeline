# Databricks notebook source
# MAGIC %md 
# MAGIC ## Clean the Data that we ingested from the inbound files, Check for null, fix types, join/clean etc.

# COMMAND ----------

# DBTITLE 1,Load the data from the table
listings = spark.read.table('taproot_training.airbnb.listings')
display(listings)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Clean up & Fix Data 
# MAGIC - Getting Rid of Extreme values for price, rooms, min nights and other numerical columns
# MAGIC - Handle null values
# MAGIC - Cast to Double all numeric values.
# MAGIC

# COMMAND ----------

display(listings.select('price').describe())

# COMMAND ----------

from pyspark.sql.functions import col,translate

listings = listings.withColumn('price',translate(col('price'), "$",'').cast("double"))
display(listings)

# COMMAND ----------

from pyspark.sql.functions import col

display(listings.filter(col('price').isNull()))

# COMMAND ----------

from pyspark.sql.functions import col
listings = listings.filter(col('price').isNotNull()).filter(col('price') > 0).filter(col('price')<1000)
display(listings)

# COMMAND ----------

display(listings.select('minimum_nights').describe())

# COMMAND ----------

display(listings.groupBy("minimum_nights").count()
        .orderBy(col("count").desc(),col("minimum_nights")))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Lets fix the minimum nights to 365 days or less.

# COMMAND ----------

listings = listings.filter(col("minimum_nights") < 365)

# COMMAND ----------

# What other rules can we add to the data to make it more cleaner. We will probably come back to it when we start doing the final model.

# COMMAND ----------

listings.write.mode('overwrite').saveAsTable('taproot_training.airbnb.listings_clean')

# COMMAND ----------



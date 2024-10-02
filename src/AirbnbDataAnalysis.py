# Databricks notebook source
# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %pip install geojson

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import geojson
j_df = spark.read.json("abfss://inbounddata@airbnbstorageaccount.dfs.core.windows.net/boston/03242024/neighbourhoods.geojson")
display(j_df)

# COMMAND ----------

cal_df = spark.read.format("csv").option("header",True).load("abfss://inbounddata@airbnbstorageaccount.dfs.core.windows.net/boston/03242024/calendar.csv.gz")
display(cal_df)

# COMMAND ----------

display(reviews_df)

# COMMAND ----------

n_df = spark.read.format("csv").option("header",True).load("abfss://inbounddata@airbnbstorageaccount.dfs.core.windows.net/boston/03242024/neighbourhoods.csv")
display(n_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

display(listings_df)

# COMMAND ----------

reviews_df = spark.read.format("csv").option("header",True).load("abfss://inbounddata@airbnbstorageaccount.dfs.core.windows.net/boston/03242024/reviews.csv")
display(reviews_df)

# COMMAND ----------

reviews_uncompr_df = spark.read.format("csv").option("header",True).load("abfss://inbounddata@airbnbstorageaccount.dfs.core.windows.net/boston/03242024/reviews.csv.gz")
display(reviews_uncompr_df)

# COMMAND ----------

r_csv_uncompre_df = spark.read.format("csv").option("header",True).option("multiline",True).load("abfss://inbounddata@airbnbstorageaccount.dfs.core.windows.net/boston/03242024/reviews_uncompressed.csv")
display(r_csv_uncompre_df)

# COMMAND ----------



# COMMAND ----------

columns_to_keep = [
    "host_is_superhost",
    "cancellation_policy",
    "instant_bookable",
    "host_total_listings_count",
    "neighbourhood_cleansed",
    "latitude",
    "longitude",
    "property_type",
    "room_type",
    "accommodates",
    "bathrooms",
    "bedrooms",
    "beds",
    "bed_type",
    "minimum_nights",
    "number_of_reviews",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "price"
]

base_df = listings_df.select(columns_to_keep)
base_df.cache().count()
display(base_df)

# COMMAND ----------



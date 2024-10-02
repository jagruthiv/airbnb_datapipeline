# Databricks notebook source
# DBTITLE 1,Set up Storage credentials from secrets

acc_name = dbutils.secrets.get(scope="airbnb_secrets",key='storage_account_name')
storage_key = dbutils.secrets.get(scope="airbnb_secrets",key='storage_key')
spark.conf.set(f"fs.azure.account.key.{acc_name}.dfs.core.windows.net",storage_key)
print("Completed setting up storage")

# COMMAND ----------

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

# Databricks notebook source
cal_df = spark.read.format("csv").option("header",True).load("abfss://inbounddata@airbnbstorageaccount.dfs.core.windows.net/boston/03242024/calendar.csv.gz")
display(cal_df)

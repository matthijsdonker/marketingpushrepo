#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 16 11:13:05 2023

@author: donker
"""

from pyspark.sql import SparkSession

def merge_and_rename_dataframes(dataset1_path, dataset2_path, country_filter):
    # Creating SparkSession-object
    spark = SparkSession.builder.appName('pySparkSetup').getOrCreate()

    # Loading datasets in DataFrames
    df_1 = spark.read.csv(dataset1_path)
    df_2 = spark.read.csv(dataset2_path)

    # Selecting clients from a specific country
    df_nl = df_1.filter(df_1["_c4"] == country_filter)

    # Deleting personal identifiable information from first dataset, 
    # excluding emails
    df_cleaned = df_nl.drop("_c1", "_c2")

    # Deleting creditcardnumbers from second dataset
    df_2_cleaned = df_2.drop("_c3")

    # Merging DataFrames using the id field
    merged_df = df_cleaned.join(df_2_cleaned, on="_c0", how="inner")

    # Renaming columns for better readability
    df = merged_df.withColumnRenamed("_c0", "client_identifier") \
                  .withColumnRenamed("_c4", "country") \
                  .withColumnRenamed("_c1", "bitcoin_address") \
                  .withColumnRenamed("_c2", "credit_card_type") \
                  .withColumnRenamed("_c3", "email")

    return df

# Example of using the function
dataset1_path = '/Users/donker/Desktop/dataset_one.csv'
dataset2_path = '/Users/donker/Desktop/dataset_two.csv'
country_filter = "Netherlands"

result_df = merge_and_rename_dataframes(dataset1_path, dataset2_path, country_filter)
result_df.show()
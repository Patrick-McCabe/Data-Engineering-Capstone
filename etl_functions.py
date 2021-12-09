# Do all imports and installs here
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
from pyspark.sql import SparkSession
import os
import glob
import configparser
from datetime import datetime, timedelta
from pyspark.sql import types as t
from pyspark.sql.functions import udf,to_date, date_add, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
import pyspark.sql.functions as f

def add_raceCount_to_demographics(spark, demographics_df):
    #Create a new dataframe called cityStateDuplicate to display the number of times a city and state combination is duplicated. 
    cityStateDuplicate = demographics_df.groupby(["City", "State"]).size().reset_index(name="Unique_Races")

    #Create new dataframe called demographics_df_WithRaceCount that includes the number of unique race values for each City State combination.
    demographics_df_WithRaceCount = demographics_df.merge(cityStateDuplicate, how = 'left', on = ['City', 'State'])
    

    #Create new dataframe to pivot races into column headers.  This data frame has one row for each city and five columns, one for each race.
    spark_demographics_df_WithRaceCount = spark.createDataFrame(demographics_df_WithRaceCount)
    City_Race_Table = spark_demographics_df_WithRaceCount.groupBy(["City", "State"]).pivot('Race').sum('Count')

    return City_Race_Table

def add_arrivalDate_CityState_to_immigration_df(spark, immigration_df):
    immigration_df.createOrReplaceTempView('immigration_fact_table')
    
    #The 'arrdate' field indicates days since Jan. 1 1960 which is converted here to a usable date format and added to a new column titled 'arrival date'.
    revised_immigration_df = spark.sql("SELECT *, date_add(to_date('1960-01-01'), arrdate) AS arrival_date FROM immigration_fact_table")
    
    #Read in i94PortsOfEntryCodes.csv
    i94PortsOfEnrtyCodes = (spark.read.format("csv").options(header="true").load("i94PortsOfEntryCodes.csv"))
    i94PortsOfEnrtyCodes.createOrReplaceTempView('i94PortsOfEnrtyCodes_spark_view')
    
    revised_immigration_df.createOrReplaceTempView('revised_immigration')

    revised_df = spark.sql("""
        SELECT revised_immigration.*, i94PortsOfEnrtyCodes_spark_view.location AS port_of_entry_city, i94PortsOfEnrtyCodes_spark_view.state AS port_of_entry_state
        FROM revised_immigration
        INNER JOIN i94PortsOfEnrtyCodes_spark_view
        ON revised_immigration.i94port = i94PortsOfEnrtyCodes_spark_view.code
    """)
    
    return revised_df

def clean_temp_df(spark, temperature_df):
    #Convert the dt column to a date data type
    temperature_df['dt'] = pd.to_datetime(temperature_df['dt'])
    
    #Drop where the Country column does not equal "United States"
    temperature_df = temperature_df[temperature_df['Country'] == 'United States']

    #Drop rows containing a null value in the AverageTemperature column
    temperature_df = temperature_df.dropna(subset=['AverageTemperature'])
    
    return temperature_df
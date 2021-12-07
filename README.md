# Data-Engineering-Capstone

#### Project Summary
This project is focused on an extract, transform, and load (ETL) process that incorporates I94 immigration, demographic and temperature data of cities within the United States.  Raw data is staged, cleaned, and assembled into a data model for further analysis. 

The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
    * I94 Immigration Data
    * US Cities Demographics
    * World Temperature Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Project Scope 
This project utilizes pandas software library and the Spark Python API (PySpark) to assemble raw data from three sources into a relational database which can easily be queried for analysis. Data is extracted from csv and sas files and staged within dataframes.  An analysis of each dataframe is performed to determine the structure, flaws, and characteristics of each dataframe.  A cleaning process resolves any identified flaws which may include missing values, duplicate rows, erroneous data etc.  
    
The dataframes are eventually  split into tables, compiling a data model used for ongoing data analysis. The data model was designed with a focus on US immigration and US city demographics and US temperatures.  Data for areas outside the US have been dropped during the cleaning process. End users are able to query the data to determine where immigration is focused and analyze those areas with regard to their historic temperatures and social demographics as of 2015. 

#### Description of Data Gathered
* __I94 Immigration Data:__ This data comes from the US National Tourism and Trade Office. To enter the United States, all non-U.S. citizens from overseas countries traveling by air or sea must complete an I94 form.  The I94 forms are compiled into a database that provides a count of visitor arrivals to the United States (with stays of 1-night or more and visiting under certain visa types) to calculate U.S. travel and tourism volume exports.  
* __World Temperature Data:__ Originally compiled by the Berkeley Earth, which is affiliated with Lawrence Berkeley National Laboratory, this is a subset of a database that combines 1.6 billion temperature reports from 16 pre-existing archives.  The records start in 1750 for average land temperature and 1850 for max and min land temperatures and global ocean and land temperatures.
* __U.S. City Demographic Data:__ This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. The data comes from the US Census Bureau's 2015 American Community Survey.

#### Cleaning Steps

##### demographics_df
* Determint the number of unique race values exist for each City, State combination and add that value to new column called 'Unique_Races'
* Pivot the Race and Count values of each City, State combination into a new dataframe called 'City_Race_Table'

#### immigration_df
* Add a new column titled 'arrival_date' by converting values in column 'i94addr' from double type to data type date


#### temperature_df
* Convert the dt column to a date data type
* Drop where the Country column does not equal "United States"
* Drop rows containing a null value in the AverageTemperature column

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
The data model is composed of the fact table and dimension tables listed below.  Analysts are able to query information about US immigration patterns and demographic information about the cities where immigrants are arriving. The racial make up of US cities is found in a dedicated dimension table, allowing analysts to efficiently develope insights into a city's cultural make up without filtering duplicate rows of the city_demographics_dim table.  Immigration arrival times are also found in a dedicated table, allowing the user to efficiently filter for specific date parameters.

#### Fact Table:
_immigration_fact_

#### Dimension Tables:
* _immigration_arrival_time_dim_
* _city_demographics_dim_
* _city_races_dim_
* _temperature_dim_

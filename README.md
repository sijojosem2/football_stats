# Task Assignment Overview

## ETL Overview

### 1. Create an ETL job in Python to import all available season’s data of the English Premier League (PL) competition. For this you will need to use the free RESTful API football-data.org (consider how to manage the response in order to transform the data to fulfill the next steps).


main.py retrieves the data from the API , please note that you need to supply your own API key for data retreival. The flow recognises JSON, variable length and fixed length responses. The response is then converted to a pandas dataframe to be written into tables and/or csv. 


### 2. Download the data as a CSV file(s)

The write_to_csv flag in [input.py](https://github.com/sijojosem2/football_stats/blob/main/input.py) enables the write csv mode, the file will be saved locally

## SQL Overview

### 3. Write the DDL statements of two tables (seasons & teams) defining which columns are required to store the downloaded data.

Both these queries are found in [queries.sql](https://github.com/sijojosem2/football_stats/blob/main/queries.sql)

### 4. Make a ER diagram of how the tables will relate to each other


### 5. Create a SQL query that will output all the teams that have won at least 3 times the Premier League since 2000-08-19. Show the team’s name and how many titles it has won.

This query is found in [queries.sql](https://github.com/sijojosem2/football_stats/blob/main/queries.sql)

### 6. (EXTRA not mandatory but nice to have) Create a SQL query that will list the name of the teams that have won the Premier League, the start date of the winning season(s) and the end date of the winning season(s), considering that if a team wins consecutive seasons the winning dates should range from the start of the first winning season until the end of the last consecutive winning season.

This query is found in [queries.sql](https://github.com/sijojosem2/football_stats/blob/main/queries.sql)

### 7. (EXTRA not mandatory but nice to have) include a function in your Python code to import the English Premier League team’s data.

Already taken care in pt.1 The ETL script imports team data from the API to the table  

## Configuration Overview

### [config.py](https://github.com/sijojosem2/football_stats/blob/main/config.py)

Contains the DB configuration. The SQL alchemy will create a local instance of SQL lite and replace if exists tables as given in the input.py parameters 

### input.py

The data for the ETL is sourced from the locations provided in [input.py](https://github.com/sijojosem2/football_stats/blob/main/input.py) . The parameters provided here are as follows 
```python
      {
      "dataset_name"  : << used to designate the dataset and name the csv >>
      "request"       : << url and headers to be provided here, any additional parameters, body also should be given >>
      "write_to_csv"  : << Flag enables or disables csv creation from the pandas dataframe>> 
      "pd_dataframe"  : <<pandas dataframe parameters, for JSON responses, provide the column that needs to be extracted in 'record_path' 
                          for non JSON responses provide either the column delimiter (variable) or colspecs(fixed length) >>
      "target_table"  : <<target table parameters given here>>
      }
```
 The api key is exported in the env 


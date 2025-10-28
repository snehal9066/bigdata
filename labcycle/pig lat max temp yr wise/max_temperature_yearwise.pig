-- Load the dataset, skip the header line using a filter
weather = LOAD '/home/cusat/Desktop/weather_data.txt' USING PigStorage(',') 
          AS (year:chararray, month:chararray, temperature:int);

-- Remove header line if it exists
weather_no_header = FILTER weather BY year != 'Year';

-- Group the data by year
grouped = GROUP weather_no_header BY year;

-- Find maximum temperature per year
max_temp = FOREACH grouped GENERATE 
              group AS year, 
              MAX(weather_no_header.temperature) AS max_temperature;

-- Display result in terminal
DUMP max_temp;


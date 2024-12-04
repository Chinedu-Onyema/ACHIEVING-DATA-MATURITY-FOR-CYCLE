-- SQL SCRIPTS TO JOIN DIFFERENT TABLES IN BIG QUERY
-- EDIT THE SQL SCRIPTS BEFORE EXECUTING THE SQL QUERY.

SELECT
TRI.usertype,
 ZIPSTART.zip_code AS zip_code_start,
 ZIPSTARTNAME.borough AS borough_start,
 ZIPSTARTNAME.neighborhood AS neighborhood_start,
  ZIPEND.zip_code AS zip_code_end,
  ZIPENDNAME.borough AS borough_end,
 ZIPENDNAME.neighborhood AS neighborhood_end,
  DATE_ADD(DATE(TRI.starttime), INTERVAL 5 YEAR) AS start_day,
  DATE_ADD(DATE(TRI.stoptime), INTERVAL 5 YEAR) AS stop_day,
  WEA.temp AS day_mean_temperature, -- Mean temp
 WEA.wdsp AS day_mean_wind_speed, -- Mean wind speed
  WEA.prcp AS day_total_precipitation, -- Total precipitation
 -- Group trips into 10 minute intervals to reduces the number of rows
  ROUND(CAST(TRI.tripduration / 60 AS INT64), -1) AS trip_minutes,
  COUNT(TRI.bikeid) AS trip_count
FROM
 `bigquery-public-data.new_york_citibike.citibike_trips` TRI
INNER JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPSTART
 ON ST_WITHIN(
 ST_GEOGPOINT(TRI.start_station_longitude, TRI.start_station_latitude),
ZIPSTART.zip_code_geom)
INNER JOIN
  `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPEND
  ON ST_WITHIN(
ST_GEOGPOINT(TRI.end_station_longitude, TRI.end_station_latitude),
ZIPEND.zip_code_geom)
INNER JOIN
`bigquery-public-data.noaa_gsod.gsod20*`  WEA
  ON PARSE_DATE("%Y%m%d", CONCAT(WEA.year, WEA.mo, WEA.da)) = DATE(TRI.starttime)
INNER JOIN
  -- Note! Add your zip code table name, enclosed in backticks: `example_table`
  `public-dataset-project-898989.cyclistic.zip_codes`  ZIPSTARTNAME
  ON ZIPSTART.zip_code = CAST(ZIPSTARTNAME.zip AS STRING)
INNER JOIN
  -- Note! Add your zipcode table name, enclosed in backticks: `example_table`
  `public-dataset-project-898989.cyclistic.zip_codes` ZIPENDNAME
  ON ZIPEND.zip_code = CAST(ZIPENDNAME.zip AS STRING)
WHERE
 -- This takes the weather data from one weather station
 WEA.wban = '94728' -- NEW YORK CENTRAL PARK
 -- Use data from 2014 and 2015
 AND EXTRACT(YEAR FROM DATE(TRI.starttime)) BETWEEN 2014 AND 2015
GROUP BY
 1, 
 2,
 3,
 4,
 5,
 6,
 7,
 8,
 9,
 10,
 11,
 12,
 13;

-- NOTE: When using GROUP BY functions for in BIGQUERY after counting your columns to be grouped by always add + 1 (e.g 12 + 1) also note that 20* stands for 2000. 




-- SQL SCRIPTS TO GET THE SUMMER TRIPS

SELECT
 TRI.usertype,
 TRI.start_station_longitude,
 TRI.start_station_latitude,
 TRI.end_station_longitude,
 TRI.end_station_latitude,
 ZIPSTART.zip_code AS zip_code_start,
 ZIPSTARTNAME.borough borough_start,
 ZIPSTARTNAME.neighborhood AS neighborhood_start,
 ZIPEND.zip_code AS zip_code_end,
  ZIPENDNAME.borough borough_end,
  ZIPENDNAME.neighborhood AS neighborhood_end,
 -- Since we're using trips from 2014 and 2015, we will add 5 years to make it look recent
  DATE_ADD(DATE(TRI.starttime), INTERVAL 5 YEAR) AS start_day,
 DATE_ADD(DATE(TRI.stoptime), INTERVAL 5 YEAR) AS stop_day,
  WEA.temp AS day_mean_temperature, -- Mean temp
 WEA.wdsp AS day_mean_wind_speed, -- Mean wind speed
  WEA.prcp AS day_total_precipitation, -- Total precipitation
  -- We will group trips into 10 minute intervals, which also reduces the number of rows 
 ROUND(CAST(TRI.tripduration / 60 AS INT64), -1) AS trip_minutes,
COUNT(TRI.bikeid) AS trip_count
FROM  
 `bigquery-public-data.new_york_citibike.citibike_trips` TRI
INNER JOIN
`bigquery-public-data.geo_us_boundaries.zip_codes` ZIPSTART
ON ST_WITHIN(
ST_GEOGPOINT(TRI.start_station_longitude, TRI.start_station_latitude),
 ZIPSTART.zip_code_geom)
INNER JOIN
`bigquery-public-data.geo_us_boundaries.zip_codes`  ZIPEND
ON ST_WITHIN(
 ST_GEOGPOINT(TRI.end_station_longitude, TRI.end_station_latitude),
ZIPEND.zip_code_geom)
INNER JOIN-- https://pantheon.corp.google.com/bigquery?p=bigquery-public-data&d=noaa_gsod
 `bigquery-public-data.noaa_gsod.gsod20*` WEA
 ON PARSE_DATE("%Y%m%d", CONCAT(WEA.year, WEA.mo, WEA.da)) = DATE(TRI.starttime)
INNER JOIN
-- Note! Add your zipcode table name, enclosed in backticks: `example_table`
`public-dataset-project-898989.cyclistic.zip_codes`  ZIPSTARTNAME
ON ZIPSTART.zip_code = CAST(ZIPSTARTNAME.zip AS STRING)
INNER JOIN
 -- Note! Add your zipcode table name below, enclosed in backticks: `example_table`
  `public-dataset-project-898989.cyclistic.zip_codes`  ZIPENDNAME
   ON ZIPEND.zip_code = CAST(ZIPENDNAME.zip AS STRING)
WHERE
-- Take the weather from one weather station
  WEA.wban = '94728' -- NEW YORK CENTRAL PARK
 -- Use data for three summer months
AND DATE(TRI.starttime) BETWEEN DATE('2015-07-01') AND DATE('2015-09-30')
GROUP BY 
1,
2,
3,
4,
5,
6,
7,
8,
9,
10,
11,
12,
13,
14,
15,
16,
17;

-- NOTE: When using GROUP BY functions for in BIGQUERY after counting your columns to be grouped by always add + 1 (e.g 16 + 1) also note that 20* stands for 2000.

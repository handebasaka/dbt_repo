WITH forecast_hour_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_hour')}}
),
add_features AS (
    SELECT *
        ,date_time::time AS time -- only time (hours:minutes:seconds) as TIME data type
        ,DATE_PART('HH24:MI', time) as hour -- time (hours:minutes) as TEXT data type
        ,TO_CHAR(time, 'Month') AS month_of_year -- month name as a text
        ,TO_CHAR(time, 'Day') AS day_of_week -- weekday name as text
    FROM forecast_hour_data
)
SELECT *
FROM add_features
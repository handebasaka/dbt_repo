-- Goal: A table showing monthly insights.

WITH joining_month_location AS (
    SELECT * FROM {{ref('prep_forecast_day')}}
    LEFT JOIN {{ref('staging_location')}}
    USING(city, region, country)
),
filtering_features AS (
    SELECT
        year_and_month,
        month_of_year,
        year,
        city,
        region,
        country,
        lat,
        lon,
        max_temp_c,
        min_temp_c,
        avg_temp_c,
        max_wind_kph,
        total_precip_mm,
        total_snow_cm,
        avg_humidity,
        daily_will_it_rain,
        daily_chance_of_rain,
        daily_will_it_snow,
        daily_chance_of_snow
    FROM joining_month_location
),
aggregations_adding_features AS (
    SELECT
        year_and_month,
        month_of_year,
        year,
        city,
        region,
        country,
        lat,
        lon,
        MAX(max_temp_c) AS max_temp_c,
        MIN(min_temp_c) AS min_temp_c,
        AVG(avg_temp_c) AS avg_temp_c,
        AVG(max_wind_kph) AS avg_max_wind_kph,
        SUM(total_precip_mm) AS total_precip_mm,
        SUM(total_snow_cm) AS total_snow_cm,
        AVG(avg_humidity) AS avg_humidity,
        SUM(daily_will_it_rain) AS will_it_rain_days,
        AVG(daily_chance_of_rain) AS daily_chance_of_rain_avg,
        SUM(daily_will_it_snow) AS will_it_snow_days,
        AVG(daily_chance_of_snow) AS daily_chance_of_snow_avg
    FROM filtering_features
    GROUP BY (year_and_month, month_of_year, year, city, region, country, lat, lon)
    ORDER BY city, year_and_month
)
SELECT * FROM aggregations_adding_features
create_curated_current = ("""CREATE TABLE IF NOT EXISTS current_weather (
    date_local date,
    timestamp_local timestamp,
    location text,
    city text,
    state text,
    country text,
    latitude real,
    longitude real,
    timezone text,
    temperature real,
    thermal_sensation real,
    precipitation real,
    humidity real,
    cloud_cover real,
    uv_radiation real,
    wind_speed real,
    wind_direction text,
    condition text,
    updatetime_utc timestamp);""")

update_curated_current = """INSERT INTO current_weather (
    date_local,
    timestamp_local,
    location,
    city,
    state,
    country,
    latitude,
    longitude,
    timezone,
    temperature,
    thermal_sensation,
    precipitation,
    humidity,
    cloud_cover,
    uv_radiation,
    wind_speed,
    wind_direction,
    condition,
    updatetime_utc
)
SELECT
    DATE(cast(local_time as timestamp)),
    cast(local_time as timestamp),
    UPPER(query),
    UPPER(name),
    UPPER(region),
    UPPER(country),
    cast(lat as real),
    cast(lon as real),
    tz_id,
    cast(temp_c as real),
    cast(feelslike_c as real),
    cast(precip_mm as real),
    cast(humidity as real),
    cast(cloud as real),
    cast(uv as real),
    cast(wind_kph as real),
    wind_dir,
    REPLACE(condition, '''', '"')::jsonb->>'text',
    cast(update_time as timestamp)
FROM
    current_weather_raw;"""

create_curated_timeline = """CREATE TABLE IF NOT EXISTS timeline_weather (
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    latitude REAL,
    longitude REAL,
    timezone TEXT,
    type TEXT,
    time_reference TIMESTAMP,
    date DATE,
    hour INTEGER,
    temperature REAL,
    thermal_sensation REAL,
    humidity REAL,
    uv_radiation REAL,
    visibility REAL,
    condition TEXT,
    wind_speed REAL,
    wind_direction TEXT);"""

update_curated_timeline = """ INSERT INTO timeline_weather(
WITH WeatherData AS (
    SELECT
	    update_time,
		local_time,
		query,
		name,
		region,
		country,
		lat,
		lon,
		tz_id,
        date,
	    'historical' as type,
        day::jsonb AS weather_data,
        replace(hour, '''', '"')::jsonb AS hour
    FROM
        history_weather_raw
	
union all 
	
	 SELECT
	    update_time,
		local_time,
		query,
		name,
		region,
		country,
		lat,
		lon,
		tz_id,
        date,
	    'forecast' as type,
        day::jsonb AS weather_data,
        replace(hour, '''', '"')::jsonb AS hour
    FROM
        forecast_weather_raw
	
),

WeatherDataExploded AS (
SELECT
	UPPER(query) id,
    UPPER(name) city,
    UPPER(region) state,
    UPPER(country) country,
    cast(lat as real) latitude,
    cast(lon as real) longitude,
    tz_id timezone,
	type,
	cast((jsonb_array_elements(hour))->>'time' as timestamp) AS time_reference,
    date(cast((jsonb_array_elements(hour))->>'time' as timestamp)) AS date,
	extract('hour' from cast((jsonb_array_elements(hour))->>'time' as timestamp)) AS hour,
    (jsonb_array_elements(hour)->>'temp_c')::real AS temperature,
    (jsonb_array_elements(hour)->>'feelslike_c')::real AS thermal_sensation,
    (jsonb_array_elements(hour)->>'humidity')::real AS humidity,
    (jsonb_array_elements(hour)->>'uv')::real AS uv_radiation,
    (jsonb_array_elements(hour)->>'vis_km')::real AS visibility,
    (jsonb_array_elements(hour)->>'condition')::jsonb->>'text' AS condition,
    (jsonb_array_elements(hour)->>'wind_kph')::real AS wind_speed,
    (jsonb_array_elements(hour)->>'wind_dir') AS wind_direction
    
FROM
    WeatherData

)

SELECT *
FROM WeatherDataExploded 
order by id,date,hour,type

)
"""
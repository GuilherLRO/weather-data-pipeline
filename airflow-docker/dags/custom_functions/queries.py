current_to_curated = ("""SELECT uuid as uuid,
	cast(local_time as timestamp) at time zone tz_id as local_time,
	cast(last_updated as timestamp) at time zone tz_id as last_updated,
	tz_id as timezone,
	query as location,
	country as country,
	region as state,
	name as city,
	cast(lat as float) as latitude,
	cast(lon as float) as longitude,
	cast(feelslike_c as float) as feelslike_c,
	cast(wind_kph as float) as wind_kph,
	cast(wind_degree as float) as wind_angle,
	wind_dir as wind_direction,
	cast(pressure_in as float) as pressure_in,
	cast(precip_mm as float) as precipitation_mm,
	cast(humidity as float) as humidity,
	cast(cloud as float) as cloud_cover,
	cast(vis_km as float) as visibility_km,
	cast(uv as float) as uv_index
FROM 
	public.current_weather_raw""")
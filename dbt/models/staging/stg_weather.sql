select
    cast(factory_id as string) as factory_id,
    cast(timestamp as timestamp) as timestamp,
    cast(temperature_c as double) as temperature_c,
    cast(relative_humidity_pct as double) as relative_humidity_pct,
    cast(precipitation_mm as double) as precipitation_mm,
    cast(surface_pressure_hpa as double) as surface_pressure_hpa,
    cast(wind_speed_kmh as double) as wind_speed_kmh

from delta.`/Volumes/smart_manufacturing_classic/lakehouse/silver_volume/weather`
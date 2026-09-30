select
    cast(factory_id as string) as factory_id,
    cast(factory_name as string) as factory_name,
    cast(city as string) as city,
    cast(country as string) as country,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude

from delta.`/Volumes/smart_manufacturing_classic/lakehouse/silver_volume/factories`
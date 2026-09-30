select
    cast(machine_id as string) as machine_id,
    cast(factory_id as string) as factory_id,
    cast(machine_type as string) as machine_type,
    cast(installation_date as date) as installation_date,
    cast(rated_power_kw as double) as rated_power_kw,
    cast(status as string) as status

from delta.`/Volumes/smart_manufacturing_classic/lakehouse/silver_volume/machines`
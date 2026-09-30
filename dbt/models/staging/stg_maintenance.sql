select
    cast(maintenance_id as string) as maintenance_id,
    cast(machine_id as string) as machine_id,
    cast(maintenance_date as timestamp) as maintenance_date,
    cast(maintenance_type as string) as maintenance_type,
    cast(failure_type as string) as failure_type,
    cast(downtime_minutes as int) as downtime_minutes,
    cast(technician as string) as technician,
    cast(cost as double) as cost

from delta.`/Volumes/smart_manufacturing_classic/lakehouse/silver_volume/maintenance`
select
    cast(production_id as string) as production_id,
    cast(machine_id as string) as machine_id,
    cast(product_id as string) as product_id,
    cast(production_start as timestamp) as production_start,
    cast(production_end as timestamp) as production_end,
    cast(quantity_produced as bigint) as quantity_produced

from delta.`/Volumes/smart_manufacturing_classic/lakehouse/silver_volume/production`
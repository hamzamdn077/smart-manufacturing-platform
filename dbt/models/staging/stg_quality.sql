select
    cast(inspection_id as string) as inspection_id,
    cast(production_id as string) as production_id,
    cast(machine_id as string) as machine_id,
    cast(inspection_timestamp as timestamp) as inspection_timestamp,
    cast(quality_score as double) as quality_score,
    cast(defect_type as string) as defect_type,
    cast(passed as boolean) as passed

from delta.`/Volumes/smart_manufacturing_classic/lakehouse/silver_volume/quality`
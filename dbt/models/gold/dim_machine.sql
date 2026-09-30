select
    machine_id,
    factory_id,
    machine_type,
    installation_date,
    rated_power_kw,
    status

from {{ ref('stg_machines') }}
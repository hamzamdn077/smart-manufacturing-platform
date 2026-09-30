select
    factory_id,
    factory_name,
    city,
    country,
    latitude,
    longitude

from {{ ref('stg_factories') }}
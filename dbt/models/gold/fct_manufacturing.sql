
with production as (

    select *
    from {{ ref('stg_production') }}

),

machines as (

    select *
    from {{ ref('stg_machines') }}

),

-- to keep the latest quality inspection for each production order
quality_ranked as (

    select
        production_id,
        quality_score,
        passed,
        defect_type,
        inspection_timestamp,

        row_number() over (
            partition by production_id
            order by inspection_timestamp desc
        ) as rn

    from {{ ref('stg_quality') }}

),

quality as (

    select
        production_id,
        quality_score,
        passed,
        defect_type

    from quality_ranked
    where rn = 1

),

-- to aggregate maintenance events during production
maintenance_agg as (

    select
        p.production_id,

        coalesce(sum(m.downtime_minutes), 0) as downtime_minutes,
        coalesce(sum(m.cost), 0) as maintenance_cost

    from production p

    left join {{ ref('stg_maintenance') }} m
        on p.machine_id = m.machine_id
        and m.maintenance_date >= p.production_start
        and m.maintenance_date <= p.production_end

    group by p.production_id

),

-- to aggregate weather observations during production
weather_agg as (

    select
        p.production_id,

        avg(w.temperature_c) as temperature_c,
        avg(w.relative_humidity_pct) as relative_humidity_pct,
        avg(w.precipitation_mm) as precipitation_mm

    from production p

    left join machines m
        on p.machine_id = m.machine_id

    left join {{ ref('stg_weather') }} w
        on m.factory_id = w.factory_id
        and w.timestamp >= p.production_start
        and w.timestamp <= p.production_end

    group by p.production_id

)

select

    -- Business key
    p.production_id,

    -- Dimension keys
    cast(
        date_format(cast(p.production_start as date), 'yyyyMMdd')
        as int
    ) as date_key,

    m.factory_id,
    p.machine_id,
    p.product_id,

    -- Production information
    p.production_start,
    p.production_end,

    timestampdiff(
        MINUTE,
        p.production_start,
        p.production_end
    ) as production_duration_minutes,

    p.quantity_produced,

    -- Quality
    q.quality_score,
    q.passed,
    q.defect_type,

    -- Maintenance
    ma.downtime_minutes,
    ma.maintenance_cost,

    -- Weather
    wa.temperature_c,
    wa.relative_humidity_pct,
    wa.precipitation_mm

from production p

left join machines m
    on p.machine_id = m.machine_id

left join quality q
    on p.production_id = q.production_id

left join maintenance_agg ma
    on p.production_id = ma.production_id

left join weather_agg wa
    on p.production_id = wa.production_id
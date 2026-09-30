

with bounds as (

    select
        min(cast(production_start as date)) as min_date,
        max(cast(production_start as date)) as max_date
    from {{ ref('stg_production') }}

),

date_range as (

    select
        explode(
            sequence(
                min_date,
                max_date,
                interval 1 day
            )
        ) as date_day

    from bounds

)

select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    weekofyear(date_day) as week_of_year,
    day(date_day) as day,
    dayofweek(date_day) as day_of_week,
    date_format(date_day, 'EEEE') as day_name

from date_range
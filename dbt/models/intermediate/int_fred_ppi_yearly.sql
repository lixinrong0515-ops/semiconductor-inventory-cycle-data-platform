with semiconductor_ppi as (

    select
        date,
        value
    from {{ ref('stg_fred_semiconductor_ppi') }}

),

yearly_ppi as (

    select
        extract(year from date) as year,
        avg(value) as semiconductor_ppi
    from semiconductor_ppi
    group by year

),

calculated as (

    select
        year,
        semiconductor_ppi,

        -- Year-over-Year change
        (
            safe_divide(
                semiconductor_ppi,
                lag(semiconductor_ppi, 1) over (
                    order by year
                )
            ) - 1
        ) * 100 as ppi_yoy

    from yearly_ppi
    order by year
)

select *
from calculated

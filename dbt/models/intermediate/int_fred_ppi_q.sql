with semiconductor_ppi as (

    select
        date,
        value
    from {{ ref('stg_fred_semiconductor_ppi') }}

),

quarterly_ppi as (

    select
        extract(year from date) as year,
        extract(quarter from date) as quarter,

        concat(
            cast(extract(year from date) as string),
            '-Q',
            cast(extract(quarter from date) as string)
        ) as year_quarter,

        avg(value) as semiconductor_ppi

    from semiconductor_ppi

    group by
        year,
        quarter,
        year_quarter

),

calculated as (

    select
        year,
        quarter,
        year_quarter,
        semiconductor_ppi,

        -- Quarter-over-Quarter change
        (
            safe_divide(
                semiconductor_ppi,
                lag(semiconductor_ppi, 1) over (
                    order by year, quarter
                )
            ) - 1
        ) * 100 as ppi_qoq,

        -- Year-over-Year change
        (
            safe_divide(
                semiconductor_ppi,
                lag(semiconductor_ppi, 4) over (
                    order by year, quarter
                )
            ) - 1
        ) * 100 as ppi_yoy

    from quarterly_ppi
    order by year, quarter
)

select *
from calculated
order by year, quarter

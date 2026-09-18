with semiconductor_ppi as (

    select
        date,
        value
    from {{ ref('stg_fred_semiconductor_ppi') }}

),

calculated as (

    select
        date,
        format_date('%Y-%m', date) as month,

        value as semiconductor_ppi,

        -- Year-over-Year change
        (
            safe_divide(
                value,
                lag(value, 12) over (
                    order by date
                )
            ) - 1
        ) * 100 as ppi_yoy,

        -- Month-over-Month change
        (
            safe_divide(
                value,
                lag(value, 1) over (
                    order by date
                )
            ) - 1
        ) * 100 as ppi_mom
    
    from semiconductor_ppi
    order by date
)

select *
from calculated


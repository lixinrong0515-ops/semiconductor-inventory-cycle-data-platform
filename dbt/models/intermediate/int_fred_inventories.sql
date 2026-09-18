with inventory as (

    select
        date,
        value
    from {{ ref('stg_fred_inventories') }}

),

calculated as (

    select
        date,
        format_date('%Y-%m', date) as month,

        value as inventory,

        -- Year-over-Year change
        (
            safe_divide(
                value,
                lag(value, 12) over (
                    order by date
                )
            ) - 1
        ) * 100 as inventory_yoy,

        -- Month-over-Month change
        (
            safe_divide(
                value,
                lag(value, 1) over (
                    order by date
                )
            ) - 1
        ) * 100 as inventory_mom

    from inventory
    order by date

) 

select *
from calculated

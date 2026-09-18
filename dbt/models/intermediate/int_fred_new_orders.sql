with new_orders as (

    select
        date,
        value
    from {{ ref('stg_fred_new_orders') }}

),

calculated as (

    select
        date,
        format_date('%Y-%m', date) as month,

        value as new_orders,

        -- Year-over-Year change
        (
            safe_divide(
                value,
                lag(value, 12) over (
                    order by date
                )
            ) - 1
        ) * 100 as new_orders_yoy,

        -- Month-over-Month change
        (
            safe_divide(
                value,
                lag(value, 1) over (
                    order by date
                )
            ) - 1
        ) * 100 as new_orders_mom
    
    from new_orders
    order by date
)

select *
from calculated


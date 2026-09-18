with monthly_orders as (

    select
        date,
        value
    from {{ ref('stg_fred_new_orders') }}

),

yearly_orders as (

    select
        extract(year from date) as year,
        sum(value) as new_orders
    from monthly_orders
    group by year

),

calculated as (

    select
        year,
        new_orders,

        -- Year-over-Year change
        (
            safe_divide(
                new_orders,
                lag(new_orders, 1) over (
                    order by year
                )
            ) - 1
        ) * 100 as new_orders_yoy

    from yearly_orders
    order by year
)

select *
from calculated
order by year
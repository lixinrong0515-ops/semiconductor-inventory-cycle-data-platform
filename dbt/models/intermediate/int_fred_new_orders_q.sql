with monthly_orders as (

    select
        date,
        value
    from {{ ref('stg_fred_new_orders') }}

),

quarterly_orders as (

    select
        extract(year from date) as year,
        extract(quarter from date) as quarter,

        concat(
            cast(extract(year from date) as string),
            '-Q',
            cast(extract(quarter from date) as string)
        ) as year_quarter,

        sum(value) as new_orders

    from monthly_orders

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
        new_orders,

        -- Quarter-over-Quarter change
        (
            safe_divide(
                new_orders,
                lag(new_orders, 1) over (
                    order by year, quarter
                )
            ) - 1
        ) * 100 as new_orders_qoq,

        -- Year-over-Year change
        (
            safe_divide(
                new_orders,
                lag(new_orders, 4) over (
                    order by year, quarter
                )
            ) - 1
        ) * 100 as new_orders_yoy

    from quarterly_orders
    order by year, quarter
)

select *
from calculated
order by year, quarter


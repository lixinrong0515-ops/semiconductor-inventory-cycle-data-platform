with inventory as (

    select
        date,
        value
    from {{ ref('stg_fred_inventories') }}

),

quarterly as (

    select
        date,
        extract(year from date) as year,
        extract(quarter from date) as quarter,

        concat(
            cast(extract(year from date) as string),
            '-Q',
            cast(extract(quarter from date) as string)
        ) as year_quarter,

        value as quarterly_inventory

    from inventory

    qualify row_number() over (
        partition by extract(year from date), extract(quarter from date)
        order by date desc
    ) = 1

),

calculated as (

    select
        date,
        year,
        quarter,
        year_quarter,
        quarterly_inventory,

        -- QoQ
        (
            safe_divide(
                quarterly_inventory,
                lag(quarterly_inventory, 1) over (
                    order by year, quarter
                )
            ) - 1
        ) * 100 as inventory_qoq,

        -- YoY
        (
            safe_divide(
                quarterly_inventory,
                lag(quarterly_inventory, 4) over (
                    order by year, quarter
                )
            ) - 1
        ) * 100 as inventory_yoy

    from quarterly
    order by year, quarter

)

select *
from calculated
order by year, quarter
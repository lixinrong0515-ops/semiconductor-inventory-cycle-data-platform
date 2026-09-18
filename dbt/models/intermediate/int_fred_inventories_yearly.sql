with inventory as (

    select
        date,
        value
    from {{ ref('stg_fred_inventories') }}

),

yearly as (

    select
        date,
        extract(year from date) as year,
        value as yearly_inventory

    from inventory

    where extract(month from date) = 12

),

calculated as (

    select
        date,
        year,
        yearly_inventory,

        (
            safe_divide(
                yearly_inventory,
                lag(yearly_inventory) over (
                    order by year
                )
            ) - 1
        ) * 100 as inventory_yoy

    from yearly
    order by date

)

select *
from calculated
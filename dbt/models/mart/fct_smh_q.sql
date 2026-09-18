with smh as (

    select
        date,
        year,
        quarter,
        year_quarter,
        smh_value,
        smh_qoq,
        smh_yoy
    from {{ ref('int_smh_q') }}

),

sp500 as (

    select
        year_quarter,
        sp500_value,
        sp500_qoq,
        sp500_yoy
    from {{ ref('int_sp500_q') }}

),

new_orders as (

    select
        year_quarter,
        new_orders,
        new_orders_qoq,
        new_orders_yoy
    from {{ ref('int_fred_new_orders_q') }}

),

ppi as (

    select
        year_quarter,
        semiconductor_ppi,
        ppi_qoq,
        ppi_yoy
    from {{ ref('int_fred_ppi_q') }}

),

inventories as (

    select
        year_quarter,
        quarterly_inventory,
        inventory_qoq,
        inventory_yoy
    from {{ ref('int_fred_inventories_q') }}

),

final as (

    select
        smh.date,
        smh.year,
        smh.quarter,
        smh.year_quarter,

        -- SMH
        smh.smh_value,
        smh.smh_qoq,
        smh.smh_yoy,

        -- S&P 500
        sp500.sp500_value,
        sp500.sp500_qoq,
        sp500.sp500_yoy,

        -- SMH relative performance vs S&P 500
        smh.smh_qoq - sp500.sp500_qoq
            as smh_vs_sp500_qoq,

        smh.smh_yoy - sp500.sp500_yoy
            as smh_vs_sp500_yoy,

        -- New Orders
        new_orders.new_orders,
        new_orders.new_orders_qoq,
        new_orders.new_orders_yoy,

        -- Semiconductor PPI
        ppi.semiconductor_ppi,
        ppi.ppi_qoq,
        ppi.ppi_yoy,

        -- Semiconductor Inventories
        inventories.quarterly_inventory,
        inventories.inventory_qoq,
        inventories.inventory_yoy

    from smh

    left join sp500
        on smh.year_quarter = sp500.year_quarter

    left join new_orders
        on smh.year_quarter = new_orders.year_quarter

    left join ppi
        on smh.year_quarter = ppi.year_quarter

    left join inventories
        on smh.year_quarter = inventories.year_quarter
    where smh_qoq is not null and smh_yoy is not null 
    order by year, quarter
)

select *
from final
where year_quarter < (
    select max(year_quarter)
    from final
)
order by year, quarter
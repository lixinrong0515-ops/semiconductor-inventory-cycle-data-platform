with data as (

    select
        year,
        quarter,
        year_quarter,

        smh_vs_sp500_yoy,
        new_orders_yoy,
        ppi_yoy,
        inventory_yoy

    from {{ ref('fct_smh_q') }}

),

lagged as (

    select
        *,

        -- New Orders
        lag(new_orders_yoy, 4) over (
            order by year, quarter
        ) as new_orders_lag1y,

        lag(new_orders_yoy, 8) over (
            order by year, quarter
        ) as new_orders_lag2y,

        lag(new_orders_yoy, 12) over (
            order by year, quarter
        ) as new_orders_lag3y,

        lag(new_orders_yoy, 16) over (
            order by year, quarter
        ) as new_orders_lag4y,

        lag(new_orders_yoy, 20) over (
            order by year, quarter
        ) as new_orders_lag5y,

        -- PPI
        lag(ppi_yoy, 4) over (
            order by year, quarter
        ) as ppi_lag1y,

        lag(ppi_yoy, 8) over (
            order by year, quarter
        ) as ppi_lag2y,

        lag(ppi_yoy, 12) over (
            order by year, quarter
        ) as ppi_lag3y,

        lag(ppi_yoy, 16) over (
            order by year, quarter
        ) as ppi_lag4y,

        lag(ppi_yoy, 20) over (
            order by year, quarter
        ) as ppi_lag5y,

        -- Inventory
        lag(inventory_yoy, 4) over (
            order by year, quarter
        ) as inventory_lag1y,

        lag(inventory_yoy, 8) over (
            order by year, quarter
        ) as inventory_lag2y,

        lag(inventory_yoy, 12) over (
            order by year, quarter
        ) as inventory_lag3y,

        lag(inventory_yoy, 16) over (
            order by year, quarter
        ) as inventory_lag4y,

        lag(inventory_yoy, 20) over (
            order by year, quarter
        ) as inventory_lag5y

    from data

),

correlations as (

    -- New Orders
    select
        'New Orders YoY' as indicator,

        corr(smh_vs_sp500_yoy, new_orders_lag1y) as lag_1_year,
        corr(smh_vs_sp500_yoy, new_orders_lag2y) as lag_2_year,
        corr(smh_vs_sp500_yoy, new_orders_lag3y) as lag_3_year,
        corr(smh_vs_sp500_yoy, new_orders_lag4y) as lag_4_year,
        corr(smh_vs_sp500_yoy, new_orders_lag5y) as lag_5_year

    from lagged

    union all

    -- PPI
    select
        'PPI YoY' as indicator,

        corr(smh_vs_sp500_yoy, ppi_lag1y) as lag_1_year,
        corr(smh_vs_sp500_yoy, ppi_lag2y) as lag_2_year,
        corr(smh_vs_sp500_yoy, ppi_lag3y) as lag_3_year,
        corr(smh_vs_sp500_yoy, ppi_lag4y) as lag_4_year,
        corr(smh_vs_sp500_yoy, ppi_lag5y) as lag_5_year

    from lagged

    union all

    -- Inventory
    select
        'Inventory YoY' as indicator,

        corr(smh_vs_sp500_yoy, inventory_lag1y) as lag_1_year,
        corr(smh_vs_sp500_yoy, inventory_lag2y) as lag_2_year,
        corr(smh_vs_sp500_yoy, inventory_lag3y) as lag_3_year,
        corr(smh_vs_sp500_yoy, inventory_lag4y) as lag_4_year,
        corr(smh_vs_sp500_yoy, inventory_lag5y) as lag_5_year

    from lagged

)

select *
from correlations
order by indicator
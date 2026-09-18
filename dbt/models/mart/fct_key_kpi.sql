with latest_quarter as (

    select
        max(date) as latest_date

    from {{ ref('fct_smh_q') }}

),

latest as (

    select
        date,

        extract(year from date) as year,

        extract(quarter from date) as quarter,

        concat(
            cast(extract(year from date) as string),
            '-Q',
            cast(extract(quarter from date) as string)
        ) as year_quarter,

        -- Latest SMH YoY
        smh_yoy,

        -- Latest SMH vs S&P 500 YoY
        smh_vs_sp500_yoy

    from {{ ref('fct_smh_q') }}

    where date = (
        select latest_date
        from latest_quarter
    )

),

correlations as (

    select
        indicator,
        lag_1_year,
        lag_2_year,
        lag_3_year,
        lag_4_year,
        lag_5_year

    from {{ ref('fct_smh_q_correlation') }}

),

-- Convert lag columns into rows
correlation_long as (

    select
        indicator,
        'Lag 1Y' as lag_period,
        lag_1_year as correlation
    from correlations

    union all

    select
        indicator,
        'Lag 2Y' as lag_period,
        lag_2_year as correlation
    from correlations

    union all

    select
        indicator,
        'Lag 3Y' as lag_period,
        lag_3_year as correlation
    from correlations

    union all

    select
        indicator,
        'Lag 4Y' as lag_period,
        lag_4_year as correlation
    from correlations

    union all

    select
        indicator,
        'Lag 5Y' as lag_period,
        lag_5_year as correlation
    from correlations

),

ranked_correlations as (

    select
        indicator,
        lag_period,
        correlation,

        row_number() over (
            order by abs(correlation) desc
        ) as rank

    from correlation_long

    where correlation is not null

),

key_metrics as (

    select

        -- Latest quarter
        latest.date,
        latest.year,
        latest.quarter,
        latest.year_quarter,

        -- Latest SMH YoY return
        latest.smh_yoy
            as smh_return_yoy,

        -- Latest SMH relative performance vs S&P 500
        latest.smh_vs_sp500_yoy
            as smh_vs_sp500_yoy,

        -- Top Reversal Driver
        (
            select
                concat(
                    indicator,
                    ' ',
                    lag_period
                )
            from ranked_correlations
            where rank = 1
        ) as top_reversal_driver,

        (
            select correlation
            from ranked_correlations
            where rank = 1
        ) as top_reversal_driver_corr,

        -- Top Margin Driver
        (
            select
                concat(
                    indicator,
                    ' ',
                    lag_period
                )
            from ranked_correlations
            where rank = 2
        ) as top_margin_driver,

        (
            select correlation
            from ranked_correlations
            where rank = 2
        ) as top_margin_driver_corr,

    

    from latest

)

select *
from key_metrics
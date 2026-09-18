with daily_prices as (

    select *
    from {{ ref('stg_sp500_prices') }}

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

        adjusted_close

    from daily_prices

    qualify row_number() over (
        partition by
            extract(year from date),
            extract(quarter from date)
        order by date desc
    ) = 1

),

calculated as (

    select
        date,
        year,
        quarter,
        year_quarter,
        adjusted_close as sp500_value,

        -- Quarter-over-Quarter return
        (
            safe_divide(
                adjusted_close,
                lag(adjusted_close, 1) over (
                    order by year, quarter
                )
            ) - 1
        ) * 100 as sp500_qoq,

        -- Year-over-Year return
        (
            safe_divide(
                adjusted_close,
                lag(adjusted_close, 4) over (
                    order by year, quarter
                )
            ) - 1
        ) * 100 as sp500_yoy

    from quarterly
    order by year, quarter
)

select *
from calculated
order by year, quarter
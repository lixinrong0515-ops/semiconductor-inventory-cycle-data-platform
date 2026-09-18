
with daily_prices as (

    select *
    from {{ ref('stg_sp500_prices') }}

),

monthly as (

    select *
    from daily_prices

    qualify row_number() over (
        partition by date_trunc(date, month)
        order by date desc
    ) = 1

    order by date

),

calculated as (

    select
        *,
        FORMAT_DATE('%Y-%m', date) AS month,

        (
            safe_divide(
                adjusted_close,
                lag(adjusted_close) over (
                    order by date
                )
            ) - 1
        ) * 100 as monthly_return

    from monthly
    order by date
)

select *
from calculated


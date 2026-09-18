with daily_prices as (

    select *
    from {{ ref('stg_sp500_prices') }}

),

yearly as (

    select *
    from daily_prices

    qualify row_number() over (
        partition by extract(year from date)
        order by date desc
    ) = 1

),

calculated as (

    select
        extract(year from date) as year,
        date,
        adjusted_close,

        (
            safe_divide(
                adjusted_close,
                lag(adjusted_close) over (
                    order by date
                )
            ) - 1
        ) * 100 as yearly_return

    from yearly

)

select *
from calculated
order by year
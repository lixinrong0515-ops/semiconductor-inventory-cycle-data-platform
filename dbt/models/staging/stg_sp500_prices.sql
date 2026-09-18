with source_data as (

    select *
    from {{ source('semidata', 'sp500_prices') }}

),

renamed as (

    select
        -- nanoseconds timestamp → date
        date(timestamp_micros(cast(date / 1000 as int64))) as date,

        -- price information
        safe_cast(open as numeric) as open,
        safe_cast(high as numeric) as high,
        safe_cast(low as numeric) as low,
        safe_cast(close as numeric) as close,
        safe_cast(adj_close as numeric) as adjusted_close,

        -- trading volume
        safe_cast(volume as int64) as volume

    from source_data

)

select *
from renamed
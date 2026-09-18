with source_data as (

    select *
    from {{ source('semidata', 'fred_inventories') }}

),

renamed as (

    select
        -- retrieval date
        safe_cast(realtime_start as date) as realtime_start,
        safe_cast(realtime_end as date) as realtime_end,

        -- observation date
        safe_cast(date as date) as date,

        -- FRED observation value
        safe_cast(value as NUMERIC) as value

    from source_data

)

select *
from renamed
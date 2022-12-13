{{ config(materialized='view') }}

select  {{dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as trip_id, 
        cast(vendorid as integer) as vendorid,
        cast(ratecodeid as integer) as ratecodeid,
        cast(pulocationid as integer) as  pickup_locationid,
        cast(dolocationid as integer) as dropoff_locationid,
        
        -- timestamps
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_pickup_datetime as timestamp) as dropoff_datetime,

        {{ get_payment_type_description('payment_type') }} as payment_type_description, 
        cast(congestion_surcharge as numeric) as congestion_surcharge

from {{source('staging', 'yellow_tripdata') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

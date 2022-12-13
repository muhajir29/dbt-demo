
{{ config(materialized='table') }}

with trips_unioned_data as (
    select *, 
        'Yellow' as service_type 
    from {{ ref('stg_yellow_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select * from trips_unioned_data
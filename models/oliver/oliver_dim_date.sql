{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}
)

SELECT
    date_day as date_key,
    day_of_week,
    month_of_year as month,
    quarter_of_year as quarter,
    year_number as year
FROM cte_date
{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
)}}

select
    employee_id,
    first_name as employee_first_name,
    last_name as employee_last_name,
    email,
    certification_completion_id,
    PARSE_JSON(certification_json):certification_name::VARCHAR AS certification_name,
    PARSE_JSON(certification_json):certification_cost::INTEGER AS certification_cost,
    PARSE_JSON(certification_json):certification_awarded_date::DATE AS certification_awarded_date
from {{ source('oliver_landing', 'employee_certifications')}}

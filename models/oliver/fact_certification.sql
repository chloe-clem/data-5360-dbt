{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
)}}

select
    d.date_key,
    e.employee_key,
    ec.certification_name,
    ec.certification_cost

    
from {{ ref('stg_employee_certifications') }} ec
inner join {{ ref('oliver_dim_date') }} d
    on d.date_key = ec.certification_awarded_date
inner join {{ ref('oliver_dim_employee') }} e
    on e.employee_id = ec.employee_id

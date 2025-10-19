{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
    -- Date attributes
    d.date_key,
    d.day_of_week,
    d.month,
    d.quarter,
    d.year,
    
    -- Customer attributes
    c.cust_key,
    c.customer_id,
    c.first_name as customer_first_name,
    c.last_name as customer_last_name,
    c.email as customer_email,
    c.state as customer_state,
    
    -- Product attributes
    p.product_key,
    p.product_id,
    p.product_name,
    p.description as product_description,
    
    -- Employee attributes
    e.employee_key,
    e.employee_id,
    e.first_name as employee_first_name,
    e.last_name as employee_last_name,
    e.position as employee_position,
    
    -- Store attributes
    s.store_key,
    s.store_name,
    s.city as store_city,
    s.state as store_state,
    
    -- Sales measures
    f.quantity,
    f.unit_price,
    f.dollars_sold,
    

FROM {{ ref('fact_sales') }} f
INNER JOIN {{ ref('oliver_dim_date') }} d ON f.date_key = d.date_key
INNER JOIN {{ ref('oliver_dim_customer') }} c ON f.cust_key = c.cust_key
INNER JOIN {{ ref('oliver_dim_product') }} p ON f.product_key = p.product_key
INNER JOIN {{ ref('oliver_dim_employee') }} e ON f.employee_key = e.employee_key
INNER JOIN {{ ref('oliver_dim_store') }} s ON f.store_key = s.store_key
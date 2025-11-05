
WITH

Markup AS (
SELECT 
    *, 
    FIRST_VALUE(customer_id) 
                OVER (
                    PARTITION BY company_name, contact_name 
                    ORDER BY company_name
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS Result
FROM {{source('sources', 'customers')}}
), 

Removed AS (
SELECT DISTINCT
    Result
FROM Markup
), 

Final AS (
SELECT 
    c.*
FROM {{source('sources', 'customers')}} c
JOIN Removed r ON r.Result = c.customer_id
)

SELECT 
    *
FROM Final
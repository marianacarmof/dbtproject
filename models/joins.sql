WITH

Prod AS (
SELECT
    ct.category_name, 
    sp.company_name AS suppliers, 
    pd.product_name,
    pd.unit_price, 
    pd.product_id
FROM {{source ('sources', 'products')}} pd
LEFT JOIN {{source ('sources', 'suppliers')}} sp ON pd.supplier_id = sp.supplier_id
LEFT JOIN {{source ('sources', 'categories')}} ct ON pd.category_id = ct.category_id
), 

Orddetails AS (
SELECT
    pd.*, 
    od.order_id, 
    od.quantity, 
    od.discount
FROM {{ref('orderdetails')}} od
LEFT JOIN Prod pd ON pd.product_id = od.product_id
),

ordrs AS (
SELECT 
    ord.order_date, 
    ord.order_id,
    cs.company_name AS customer, 
    em.name AS employee, 
    em.age, 
    em.length_of_service
FROM {{source ('sources', 'orders')}} ord
LEFT JOIN {{ref('customers')}} cs ON cs.customer_id = ord.customer_id
LEFT JOIN {{ref('employees')}} em ON em.employee_id = ord.employee_id
LEFT JOIN {{source ('sources', 'shippers')}} sh ON sh.shipper_id = ord.ship_via
)

SELECT 
    od.*, 
    ord.order_date, 
    ord.customer, 
    ord.employee, 
    ord.age, 
    ord.length_of_service
FROM orddetails od
INNER JOIN ordrs ord ON od.order_id = ord.order_id

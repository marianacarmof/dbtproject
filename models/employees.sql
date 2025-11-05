SELECT 
    *, 
    DATE_PART(YEAR, CURRENT_DATE) - DATE_PART(YEAR,birth_date) AS age, 
    DATE_PART(YEAR, CURRENT_DATE) - DATE_PART(YEAR,hire_date) AS length_of_service, 
    first_name || ' ' || last_name AS name
FROM {{source('sources', 'employees')}}
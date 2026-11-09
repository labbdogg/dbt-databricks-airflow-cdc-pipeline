{{
  config(
    materialized = 'incremental',
    unique_key= 'employee_id'
    )
}}

SELECT  
    *,
    current_timestamp() AS processed_at
FROM
    {{ source('walmart_databricks', 'employees')}}

WHERE 1 = 1

{% if is_incremental() %}
  AND updated_timestamp >= coalesce((SELECT MAX(updated_timestamp) FROM {{ this }}), '1900-01-01')
{% endif %}
{{ config(materialized='table', file_format='delta') }}

SELECT 
    id,
    name,
    age,
    city,
    salary,
    salary_category,
    processing_time,
    source_name,
    row_id
FROM delta.`{{ var('raw_data_path', 'data/raw') }}`
{{ config(materialized='table') }}

SELECT


FROM {{ ref('clean_job_title_data') }}
LIMIT 1

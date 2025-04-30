{{ config(materialized='table') }}

SELECT *
FROM {{ source('linkedin_raw', 'LINKEDIN_JOB_API_CLEANED_DATA') }}
WHERE lower(title) LIKE '%data engineer%'
OR lower(title) LIKE '%data scientist%'
OR lower(title) LIKE '%data analyst%'

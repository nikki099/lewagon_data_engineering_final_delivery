{{ config(materialized='view') }}
-- total jobs to date by city

SELECT
JOB_CATEGORY,
CITY,
COUNT(DISTINCT ID) AS total_jobs
FROM {{ source('linkedin_int','INT_LINKEDIN_DATA') }}
GROUP BY
  JOB_CATEGORY,
  CITY
ORDER BY
  JOB_CATEGORY,
  CITY,
  total_jobs DESC

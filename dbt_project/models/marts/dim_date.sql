{{ config(materialized='table') }}

WITH date_spine AS (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('1990-01-01' as date)",
      end_date="cast('2000-01-01' as date)"
     )
  }}
)
SELECT
    DATE_DAY,
    EXTRACT(YEAR FROM DATE_DAY) AS YEAR,
    EXTRACT(MONTH FROM DATE_DAY) AS MONTH,
    EXTRACT(QUARTER FROM DATE_DAY) AS QUARTER,
    TO_CHAR(DATE_DAY, 'DAY') AS DAY_OF_WEEK
FROM date_spine

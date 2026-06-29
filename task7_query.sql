WITH us_cases_by_date AS (
  SELECT
    date,
    SUM(cumulative_confirmed) AS cases
  FROM
    `bigquery-public-data.covid19_open_data.covid19_open_data`
  WHERE
    country_name = "United States of America"
    AND date BETWEEN '2020-03-22' AND '2020-04-20'
  GROUP BY
    date
  ORDER BY
    date ASC
),
us_previous_day_comparison AS (
  SELECT
    date,
    cases,
    LAG(cases) OVER(ORDER BY date) AS previous_day
  FROM
    us_cases_by_date
)
SELECT
  date AS Date,
  cases AS Confirmed_Cases_On_Day,
  previous_day AS Confirmed_Cases_Previous_Day,
  (cases - previous_day) * 100 / previous_day AS Percentage_Increase_In_Cases
FROM
  us_previous_day_comparison
WHERE
  (cases - previous_day) * 100 / previous_day > 15

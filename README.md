# covid19-new-cases-daily-query

The following query powers [this dashboard](https://datastudio.google.com/reporting/1b28584b-0ecc-4f3f-aa50-3ea06a72728a), which is discussed in [this post](https://jonathanhunsucker.com/posts/visualizing-covid19-new-cases-daily/). Shared here for transparency, critique, and back-up. 

```sql
WITH data AS (
  SELECT date, state_name, county, confirmed_cases
  FROM `bigquery-public-data.covid19_nyt.us_counties`
),
average_cases AS (
  SELECT
    state_name,
    county,
    date,
    AVG(confirmed_cases) OVER (PARTITION BY state_name, county ORDER BY date ROWS BETWEEN 7 PRECEDING AND 0 FOLLOWING) as average_cases
  FROM data
),
new_cases AS (
  SELECT
    d1.state_name,
    d1.county,
    d1.date,
    d1.confirmed_cases - IF(d2.confirmed_cases IS NULL, 0, d2.confirmed_cases) as new_cases
  FROM data d1
  LEFT JOIN data d2 ON (d2.date = DATE_SUB(d1.date, INTERVAL 1 DAY) AND d1.state_name = d2.state_name AND d1.county = d2.county)
),
average_new_cases AS (
  SELECT
    d1.state_name,
    d1.county,
    d1.date,
    d1.average_cases - IF(d2.average_cases IS NULL, 0, d2.average_cases) as average_new_cases
  FROM average_cases d1
  LEFT JOIN average_cases d2 ON (d2.date = DATE_SUB(d1.date, INTERVAL 1 DAY) AND d1.state_name = d2.state_name AND d1.county = d2.county)
)

-- SELECT * FROM new_cases
-- SELECT * FROM average_cases

SELECT data.*, average_cases.average_cases, new_cases.new_cases, average_new_cases.average_new_cases
FROM data
LEFT JOIN average_cases USING (state_name, county, date)
LEFT JOIN new_cases USING (state_name, county, date)
LEFT JOIN average_new_cases USING (state_name, county, date)
```

##### Notes
* It's probably unnecessary to build common table expressions for each additional stat, but it was useful to work on one at a time, then bring them all together
* The lone `SELECT * FROM ...` queries were used during development
* I learned about common table expressions this year, and found them immensely useful during query development. It's particularly handy to self-encapsulate dependencies (esp. when working across multiple tables).
* Another new-to-me skill is self-joining to achieve lagged differences (learned while reading [Quip's SQL Interview Questions](https://quip.com/2gwZArKuWk7W))

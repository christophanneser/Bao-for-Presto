SELECT
  passenger_count,
  extract(
    year
    from
      pickup_datetime
  ) AS pickup_year,
  count(*)
FROM
  taxis
GROUP BY
  1,
  2

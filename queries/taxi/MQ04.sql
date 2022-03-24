SELECT
  passenger_count,
  extract(
    year
    from
      pickup_datetime
  ) AS pickup_year,
  cast(trip_distance as int) AS distance,
  count(*) AS the_count
FROM
  taxis
GROUP BY
  1,
  2,
  3
ORDER BY
  2,
  3 desc

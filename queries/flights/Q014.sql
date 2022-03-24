SELECT
  extract(
    month
    from
      flights.arr_timestamp
  ) as key0,
  extract(
    dow
    from
      flights.arr_timestamp
  ) as key1,
  COUNT(*) AS color
FROM
  flights
WHERE
  (
    (
      flights.dep_timestamp >= TIMESTAMP '1996-07-28 00:00:00'
      AND flights.dep_timestamp < TIMESTAMP '1997-05-18 00:00:00'
    )
  )
GROUP BY
  1,
  2
ORDER BY
  1,
  2

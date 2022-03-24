SELECT
  flights.dest_state as key0,
  AVG(flights.arrdelay) AS val
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
  1
ORDER BY
  1

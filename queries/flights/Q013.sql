SELECT
  COUNT(*) as val
FROM
  flights
WHERE
  (
      flights.dep_timestamp >= TIMESTAMP '1996-07-28 00:00:00'
      AND flights.dep_timestamp < TIMESTAMP '1997-05-18 00:00:00'
  )

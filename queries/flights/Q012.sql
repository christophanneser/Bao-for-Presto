SELECT
  flights.carrier_name,
  AVG(flights.depdelay) AS x,
  AVG(flights.arrdelay) AS y,
  COUNT(*) AS size
FROM
  flights
WHERE
  (
      flights.dep_timestamp >= TIMESTAMP '1996-07-26 16:30:06'
      AND flights.dep_timestamp < TIMESTAMP '1997-05-16 16:30:06'
  )
GROUP BY
  1
ORDER BY
  size DESC
LIMIT
  50

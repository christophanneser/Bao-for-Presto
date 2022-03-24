SELECT
  passenger_count,
  avg(total_amount)
FROM
  taxis
GROUP BY
  passenger_count

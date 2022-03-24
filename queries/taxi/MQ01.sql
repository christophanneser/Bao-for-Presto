SELECT
  cab_type,
  count(*)
FROM
  taxis
GROUP BY
  cab_type
